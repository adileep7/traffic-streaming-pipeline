# prefect/flow.py
from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

import subprocess as sp
from prefect import flow, task, get_run_logger


# ----------------------------
# CLI flags (prefect ignores unknown, so we parse early)
# ----------------------------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("-run_producer", type=str, default="true")
parser.add_argument("-run_bronze", type=str, default="true")
parser.add_argument("-run_silver", type=str, default="true")
parser.add_argument("-run_gold_10m", type=str, default="true")
parser.add_argument("-keep_running_seconds", type=int, default=0)
parser.add_argument(
    "-producer_path",
    type=str,
    default="",
    help="Optional absolute/relative path to producer.py",
)
args, _ = parser.parse_known_args()

RUN_PRODUCER = args.run_producer.lower() == "true"
RUN_BRONZE = args.run_bronze.lower() == "true"
RUN_SILVER = args.run_silver.lower() == "true"
RUN_GOLD_10M = args.run_gold_10m.lower() == "true"
KEEP_RUNNING_SECONDS = int(args.keep_running_seconds)

# repo and common paths
REPO = Path(__file__).resolve().parent.parent
KAFKA_DIR = REPO / "kafka"
SPARK_DIR = REPO / "spark"
DATA_DIR = REPO / "data"
CHECKPOINTS_DIR = REPO / "checkpoints"

# default candidate locations for the producer script
DEFAULT_PRODUCER_CANDIDATES = [
    KAFKA_DIR / "producer.py",
    KAFKA_DIR / "traffic_producer.py",
    REPO / "producer" / "producer.py",
    REPO / "scripts" / "producer.py",
]

# resolve producer path
if args.producer_path:
    PRODUCER_SCRIPT = (REPO / args.producer_path).resolve() if not os.path.isabs(args.producer_path) else Path(args.producer_path).resolve()
else:
    PRODUCER_SCRIPT = next((p for p in DEFAULT_PRODUCER_CANDIDATES if p.exists()), None)


# ----------------------------
# Utilities
# ----------------------------
def _find_spark_submit() -> str:
    """
    Returns a spark-submit executable path.
    Prefers the one in PATH; falls back to $SPARK_HOME/bin/spark-submit.
    """
    from shutil import which

    candidate = which("spark-submit")
    if candidate:
        return candidate

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        cand2 = Path(spark_home) / "bin" / "spark-submit"
        if cand2.exists():
            return str(cand2)

    # final fallback: rely on PATH and let it error naturally
    return "spark-submit"


def _spark_command(script_rel_path: str, extra_args: Optional[List[str]] = None) -> List[str]:
    """
    Build a spark-submit command with Delta + Kafka packages and Delta session configs.
    script_rel_path is relative to repo root (e.g., 'spark/stream_bronze.py').
    """
    cmd = [
        _find_spark_submit(),
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # (Optional but often handy) quiet a bit of noise:
        # "--conf", "spark.sql.adaptive.enabled=false",
        str(REPO / script_rel_path),
    ]
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _start_process(cmd: List[str], cwd: Optional[Path] = None, env: Optional[dict] = None) -> sp.Popen:
    """
    Starts a subprocess and returns the Popen handle.
    We pipe stdout/stderr to parent so logs show in Prefect.
    """
    return sp.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env or os.environ.copy(),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=False,  # allow raw pass-through
    )


def _check_started_or_fail(proc: sp.Popen, name: str, grace_seconds: float = 2.0):
    """
    Give the child a moment to fail fast; if it exits immediately, raise a helpful error.
    """
    time.sleep(grace_seconds)
    if proc.poll() is not None:
        raise RuntimeError(f"{name} exited immediately; check logs above.")


def _terminate_process(proc: Optional[sp.Popen], name: str, logger, sig=signal.SIGINT, wait_seconds: float = 6.0):
    """
    Try to stop a subprocess gracefully, then escalate if needed.
    """
    if not proc:
        return
    if proc.poll() is not None:
        return

    try:
        logger.info(f"Stopping {name} (SIGINT)...")
        proc.send_signal(sig)
        t0 = time.time()
        while proc.poll() is None and (time.time() - t0) < wait_seconds:
            time.sleep(0.25)
        if proc.poll() is None:
            logger.info(f"{name} still running; sending SIGTERM...")
            proc.terminate()
            t1 = time.time()
            while proc.poll() is None and (time.time() - t1) < wait_seconds:
                time.sleep(0.25)
        if proc.poll() is None:
            logger.info(f"{name} still running; sending SIGKILL.")
            proc.kill()
    except Exception as e:
        logger.warning(f"Error while stopping {name}: {e}")


# ----------------------------
# Prefect tasks
# ----------------------------
@task
def start_producer() -> sp.Popen:
    logger = get_run_logger()

    if not RUN_PRODUCER:
        logger.info("Producer disabled by flag.")
        return None  # type: ignore

    if PRODUCER_SCRIPT is None or not PRODUCER_SCRIPT.exists():
        raise FileNotFoundError(
            "Producer script not found. Pass `-producer_path=/full/path/to/producer.py` "
            "or place it at one of: "
            + ", ".join(map(str, DEFAULT_PRODUCER_CANDIDATES))
        )

    cmd = [sys.executable, str(PRODUCER_SCRIPT)]
    logger.info(f"Launching producer: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Producer")
    return proc


@task
def start_bronze() -> sp.Popen:
    logger = get_run_logger()

    if not RUN_BRONZE:
        logger.info("Bronze disabled by flag.")
        return None  # type: ignore

    script = "spark/stream_bronze.py"
    cmd = _spark_command(script)
    logger.info(f"Launching bronze: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Bronze stream")
    return proc


@task
def start_silver() -> sp.Popen:
    logger = get_run_logger()

    if not RUN_SILVER:
        logger.info("Silver disabled by flag.")
        return None  # type: ignore

    script = "spark/stream_silver.py"
    cmd = _spark_command(script)
    logger.info(f"Launching silver: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Silver stream")
    return proc


@task
def start_gold_10m() -> sp.Popen:
    logger = get_run_logger()

    if not RUN_GOLD_10M:
        logger.info("Gold (10m) disabled by flag.")
        return None  # type: ignore

    script = "spark/stream_gold_10m.py"
    cmd = _spark_command(script)
    logger.info(f"Launching gold_10m: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Gold 10m stream")
    return proc


@task
def stop_all_processes(procs: List[Optional[sp.Popen]]):
    logger = get_run_logger()
    names = ["Producer", "Bronze", "Silver", "Gold10m"]
    for name, p in zip(names, procs):
        _terminate_process(p, name, logger)


# ----------------------------
# The Flow
# ----------------------------
@flow(name="Traffic Pipeline Orchestrator")
def run_pipeline():
    logger = get_run_logger()
    logger.info(f"Repo root: {REPO}")
    logger.info(f"Data dir:  {DATA_DIR}")
    logger.info(f"Checkpoints dir: {CHECKPOINTS_DIR}")

    procs: List[Optional[sp.Popen]] = []
    try:
        # Launch in order: producer -> bronze -> silver -> gold
        if RUN_PRODUCER:
            procs.append(start_producer.submit().result())
        else:
            procs.append(None)

        # tiny gaps help with local startup races
        time.sleep(1.0)

        if RUN_BRONZE:
            procs.append(start_bronze.submit().result())
        else:
            procs.append(None)

        time.sleep(1.0)

        if RUN_SILVER:
            procs.append(start_silver.submit().result())
        else:
            procs.append(None)

        time.sleep(1.0)

        if RUN_GOLD_10M:
            procs.append(start_gold_10m.submit().result())
        else:
            procs.append(None)

        # hold the flow open if requested (0 means return immediately)
        if KEEP_RUNNING_SECONDS > 0:
            logger.info(f"Keeping flow alive for {KEEP_RUNNING_SECONDS} seconds...")
            end = time.time() + KEEP_RUNNING_SECONDS
            while time.time() < end:
                # If any critical proc has died, fail early
                for name, p in zip(["Producer", "Bronze", "Silver", "Gold10m"], procs):
                    if p is not None and p.poll() is not None:
                        raise RuntimeError(f"{name} process exited; see logs above.")
                time.sleep(1.0)
        else:
            logger.info("Flow started all processes; exiting without holding the run open.")

    finally:
        # ensure we always try to stop children
        logger.info("All processes stopping...")
        stop_all_processes.submit(procs).result()


if __name__ == "__main__":
    run_pipeline()
