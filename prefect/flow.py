from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # points to repo root
VENV_PYTHON = sys.executable  # uses the current venv's python

# Common Spark bits
SPARK_SUBMIT = "spark-submit"
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "io.delta:delta-spark_2.12:3.2.0"
)
SPARK_DELTA_CONF = [
    "--conf",
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf",
    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]

# Paths to your scripts
PRODUCER_SCRIPT = PROJECT_ROOT / "kafka" / "producer.py"
BRONZE_SCRIPT   = PROJECT_ROOT / "spark" / "stream_bronze.py"
SILVER_SCRIPT   = PROJECT_ROOT / "spark" / "stream_silver.py"
GOLD10M_SCRIPT  = PROJECT_ROOT / "spark" / "stream_gold_10m.py"


def _popen(cmd: List[str], cwd: Path) -> subprocess.Popen:
    """
    Start a long-running process in its own process group so we can kill the whole tree.
    Stdout/stderr are hooked to our console for live logs.
    """
    return subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=sys.stdout,
        stderr=sys.stderr,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )


def _kill_pgroup(p: Optional[subprocess.Popen], logger) -> None:
    if not p:
        return
    try:
        if p.poll() is None:
            logger.info(f"Stopping PID {p.pid} …")
            if os.name == "nt":
                p.terminate()
            else:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            # give it a moment
            for _ in range(30):
                if p.poll() is not None:
                    break
                time.sleep(0.1)
            if p.poll() is None:
                logger.warning("Force killing …")
                if os.name == "nt":
                    p.kill()
                else:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    except Exception as e:
        logger.warning(f"While stopping PID {getattr(p, 'pid', '?')}: {e}")


@task
def start_producer(env: Optional[dict] = None) -> subprocess.Popen:
    logger = get_run_logger()
    cmd = [str(VENV_PYTHON), str(PRODUCER_SCRIPT)]
    logger.info(f"Launching producer: {' '.join(cmd)}")
    p = _popen(cmd, PROJECT_ROOT)
    time.sleep(1.0)  # small grace to surface early errors
    if p.poll() is not None:
        raise RuntimeError("Producer exited immediately; check logs above.")
    return p


@task
def start_bronze() -> subprocess.Popen:
    logger = get_run_logger()
    cmd = [
        SPARK_SUBMIT,
        "--packages", SPARK_PACKAGES,
        *SPARK_DELTA_CONF,
        str(BRONZE_SCRIPT),
    ]
    logger.info(f"Launching bronze: {' '.join(cmd)}")
    p = _popen(cmd, PROJECT_ROOT)
    time.sleep(1.0)
    if p.poll() is not None:
        raise RuntimeError("Bronze stream exited immediately; check logs above.")
    return p


@task
def start_silver() -> subprocess.Popen:
    logger = get_run_logger()
    cmd = [
        SPARK_SUBMIT,
        "--packages", SPARK_PACKAGES,
        *SPARK_DELTA_CONF,
        str(SILVER_SCRIPT),
    ]
    logger.info(f"Launching silver: {' '.join(cmd)}")
    p = _popen(cmd, PROJECT_ROOT)
    time.sleep(1.0)
    if p.poll() is not None:
        raise RuntimeError("Silver stream exited immediately; check logs above.")
    return p


@task
def start_gold_10m() -> subprocess.Popen:
    logger = get_run_logger()
    cmd = [
        SPARK_SUBMIT,
        "--packages", SPARK_PACKAGES,
        *SPARK_DELTA_CONF,
        str(GOLD10M_SCRIPT),
    ]
    logger.info(f"Launching gold (10m): {' '.join(cmd)}")
    p = _popen(cmd, PROJECT_ROOT)
    time.sleep(1.0)
    if p.poll() is not None:
        raise RuntimeError("Gold 10m stream exited immediately; check logs above.")
    return p


@flow(name="Traffic Pipeline Orchestrator")
def run_pipeline(
    run_producer: bool = True,
    run_bronze: bool = True,
    run_silver: bool = True,
    run_gold_10m: bool = True,
    keep_running_seconds: int = 0,
):
    """
    Start/stop your local streaming stack under Prefect.

    Params:
      - run_* flags: choose which components to launch
      - keep_running_seconds: if >0, keep the flow alive that many seconds.
        If 0, it will run until you Ctrl-C or a process exits/crashes.

    All processes are torn down cleanly on error or cancellation.
    """
    logger = get_run_logger()
    procs: List[subprocess.Popen] = []

    try:
        if run_producer:
            procs.append(start_producer.submit().result())

        if run_bronze:
            procs.append(start_bronze.submit().result())

        if run_silver:
            procs.append(start_silver.submit().result())

        if run_gold_10m:
            procs.append(start_gold_10m.submit().result())

        logger.info(
            "All requested components launched. "
            "Press Ctrl-C to stop (in this terminal)."
        )

        # Wait loop: either fixed duration or until any child exits
        start = time.time()
        while True:
            # Exit if any process dies
            for p in procs:
                if p.poll() is not None:
                    raise RuntimeError(
                        f"Process PID {p.pid} exited with code {p.returncode}"
                    )
            if keep_running_seconds > 0 and (time.time() - start) >= keep_running_seconds:
                logger.info("Requested runtime reached; stopping.")
                break
            time.sleep(1.0)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; stopping all processes …")
    except Exception as e:
        logger.error(f"Flow error: {e}")
        raise
    finally:
        # stop in reverse order to reduce backpressure
        for p in reversed(procs):
            _kill_pgroup(p, logger)
        logger.info("All processes stopped.")


if __name__ == "__main__":
    # Quick local run: python prefect/flow.py
    run_pipeline()
