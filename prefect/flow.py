# ----------------------------------------
# Traffic Pipeline Orchestration with Prefect
# ----------------------------------------
# This script defines a Prefect flow to orchestrate a real-time data pipeline.
# It manages the coordinated execution of:
#   - Traffic data producer (fetching from Azure Maps)
#   - Spark streaming jobs (bronze, silver, gold layers)
# It includes CLI overrides, subprocess management, and graceful shutdown.

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

# ----------------------------------------
# CLI Argument Parsing
# ----------------------------------------
# Prefect ignores unknown CLI args, so we parse early to customize flow execution.
# Users can toggle stages on/off or provide custom paths.

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

# Convert CLI flags to booleans
RUN_PRODUCER = args.run_producer.lower() == "true"
RUN_BRONZE = args.run_bronze.lower() == "true"
RUN_SILVER = args.run_silver.lower() == "true"
RUN_GOLD_10M = args.run_gold_10m.lower() == "true"
KEEP_RUNNING_SECONDS = int(args.keep_running_seconds)

# ----------------------------------------
# Path Resolution
# ----------------------------------------
# Resolve script and directory paths relative to repo structure

REPO = Path(__file__).resolve().parent.parent
KAFKA_DIR = REPO / "kafka"
SPARK_DIR = REPO / "spark"
DATA_DIR = REPO / "data"
CHECKPOINTS_DIR = REPO / "checkpoints"

# Default locations to search for the producer script if not provided explicitly
DEFAULT_PRODUCER_CANDIDATES = [
    KAFKA_DIR / "producer.py",
    KAFKA_DIR / "traffic_producer.py",
    REPO / "producer" / "producer.py",
    REPO / "scripts" / "producer.py",
]

# Resolve producer script location
if args.producer_path:
    PRODUCER_SCRIPT = (
        (REPO / args.producer_path).resolve()
        if not os.path.isabs(args.producer_path)
        else Path(args.producer_path).resolve()
    )
else:
    PRODUCER_SCRIPT = next((p for p in DEFAULT_PRODUCER_CANDIDATES if p.exists()), None)

# ----------------------------------------
# Utilities
# ----------------------------------------

def _find_spark_submit() -> str:
    """
    Locate the `spark-submit` executable, preferring PATH, falling back to SPARK_HOME.
    Ensures the flow works on any environment with Spark installed.
    """
    from shutil import which

    candidate = which("spark-submit")
    if candidate:
        return candidate

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        fallback = Path(spark_home) / "bin" / "spark-submit"
        if fallback.exists():
            return str(fallback)

    return "spark-submit"  # Final fallback (may fail if misconfigured)


def _spark_command(script_rel_path: str, extra_args: Optional[List[str]] = None) -> List[str]:
    """
    Construct a full `spark-submit` command with required Delta + Kafka packages.
    Configures Spark session with Delta-specific extensions.
    """
    cmd = [
        _find_spark_submit(),
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        str(REPO / script_rel_path),
    ]
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _start_process(cmd: List[str], cwd: Optional[Path] = None, env: Optional[dict] = None) -> sp.Popen:
    """
    Launch a subprocess (producer or Spark job).
    Logs are piped directly to stdout/stderr for visibility in Prefect UI.
    """
    return sp.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env or os.environ.copy(),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=False,  # Preserve raw output (important for logs)
    )


def _check_started_or_fail(proc: sp.Popen, name: str, grace_seconds: float = 2.0):
    """
    Wait briefly after starting a process to catch immediate failures.
    Useful for catching misconfigurations early (e.g., bad Spark script).
    """
    time.sleep(grace_seconds)
    if proc.poll() is not None:
        raise RuntimeError(f"{name} exited immediately; check logs above.")


def _terminate_process(proc: Optional[sp.Popen], name: str, logger, sig=signal.SIGINT, wait_seconds: float = 6.0):
    """
    Attempt graceful termination of a subprocess, escalating if needed.
    This avoids orphaned processes and ensures a clean shutdown.
    """
    if not proc or proc.poll() is not None:
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

# ----------------------------------------
# Prefect Tasks (each step is isolated and retryable)
# ----------------------------------------

@task
def start_producer() -> sp.Popen:
    """Start the traffic incident data producer as a subprocess."""
    logger = get_run_logger()

    if not RUN_PRODUCER:
        logger.info("Producer disabled by CLI flag.")
        return None

    if PRODUCER_SCRIPT is None or not PRODUCER_SCRIPT.exists():
        raise FileNotFoundError(
            "Producer script not found. Provide -producer_path or place it in a known location."
        )

    cmd = [sys.executable, str(PRODUCER_SCRIPT)]
    logger.info(f"Launching producer: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Producer")
    return proc


@task
def start_bronze() -> sp.Popen:
    """Start the Bronze (raw ingestion) Spark streaming job."""
    logger = get_run_logger()

    if not RUN_BRONZE:
        logger.info("Bronze stream disabled by CLI flag.")
        return None

    script = "spark/stream_bronze.py"
    cmd = _spark_command(script)
    logger.info(f"Launching bronze: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Bronze stream")
    return proc


@task
def start_silver() -> sp.Popen:
    """Start the Silver (cleaned data) Spark streaming job."""
    logger = get_run_logger()

    if not RUN_SILVER:
        logger.info("Silver stream disabled by CLI flag.")
        return None

    script = "spark/stream_silver.py"
    cmd = _spark_command(script)
    logger.info(f"Launching silver: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Silver stream")
    return proc


@task
def start_gold_10m() -> sp.Popen:
    """Start the Gold (aggregated data) Spark streaming job."""
    logger = get_run_logger()

    if not RUN_GOLD_10M:
        logger.info("Gold 10m stream disabled by CLI flag.")
        return None

    script = "spark/stream_gold_10m.py"
    cmd = _spark_command(script)
    logger.info(f"Launching gold_10m: {' '.join(cmd)}")
    proc = _start_process(cmd, cwd=REPO)
    _check_started_or_fail(proc, "Gold 10m stream")
    return proc


@task
def stop_all_processes(procs: List[Optional[sp.Popen]]):
    """Stop all subprocesses cleanly when the flow ends."""
    logger = get_run_logger()
    names = ["Producer", "Bronze", "Silver", "Gold10m"]
    for name, proc in zip(names, procs):
        _terminate_process(proc, name, logger)

# ----------------------------------------
# Prefect Flow
# ----------------------------------------

@flow(name="Traffic Pipeline Orchestrator")
def run_pipeline():
    """
    Main orchestration logic.
    Starts selected pipeline components in order and monitors them.
    Ensures a graceful shutdown and failure detection.
    """
    logger = get_run_logger()
    logger.info(f"Repo root: {REPO}")
    logger.info(f"Data dir:  {DATA_DIR}")
    logger.info(f"Checkpoints dir: {CHECKPOINTS_DIR}")

    procs: List[Optional[sp.Popen]] = []

    try:
        # Launch components in order with small delays to avoid startup race conditions
        procs.append(start_producer.submit().result() if RUN_PRODUCER else None)
        time.sleep(1.0)

        procs.append(start_bronze.submit().result() if RUN_BRONZE else None)
        time.sleep(1.0)

        procs.append(start_silver.submit().result() if RUN_SILVER else None)
        time.sleep(1.0)

        procs.append(start_gold_10m.submit().result() if RUN_GOLD_10M else None)

        # Keep flow alive for interactive/debugging if specified
        if KEEP_RUNNING_SECONDS > 0:
            logger.info(f"Keeping flow alive for {KEEP_RUNNING_SECONDS} seconds...")
            end = time.time() + KEEP_RUNNING_SECONDS
            while time.time() < end:
                # Watch for unexpected process deaths
                for name, proc in zip(["Producer", "Bronze", "Silver", "Gold10m"], procs):
                    if proc and proc.poll() is not None:
                        raise RuntimeError(f"{name} process exited early; see logs.")
                time.sleep(1.0)
        else:
            logger.info("Flow launched all processes; exiting immediately (no hold).")

    finally:
        # Always attempt to shut down processes
        logger.info("Initiating cleanup of all subprocesses...")
        stop_all_processes.submit(procs).result()


# CLI entrypoint
if __name__ == "__main__":
    run_pipeline()
