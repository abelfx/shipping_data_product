import os
import sys
import subprocess
from pathlib import Path

from dagster import op, job, ScheduleDefinition, Definitions, Failure, HookContext

ROOT = Path(__file__).parent
PROJECT_DIR = ROOT / "medical_warehouse"


def run_cmd(cmd, cwd: Path | None = None, env: dict | None = None):
    """Run a shell command and raise on failure."""
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env or os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result


@op(description="Scrape Telegram channels and download images")
def scrape_telegram_data(context):
    context.log.info("Starting Telegram scrape...")
    cmd = [sys.executable, str(ROOT / "src" / "scraper.py")]
    result = run_cmd(cmd, cwd=ROOT)
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Failure(description="Scraper failed")
    context.log.info("Scrape completed.")


@op(description="Load raw JSON messages into Postgres (raw.telegram_messages)")
def load_raw_to_postgres(context):
    context.log.info("Loading raw JSON into Postgres...")
    cmd = [sys.executable, str(ROOT / "src" / "load_raw_to_postgres.py")]
    result = run_cmd(cmd, cwd=ROOT)
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Failure(description="Load to Postgres failed")
    context.log.info("Load completed.")


@op(description="Run dbt transformations and tests")
def run_dbt_transformations(context):
    env = os.environ.copy()
    context.log.info("Running dbt models...")
    run = run_cmd(
        [
            "dbt",
            "run",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(PROJECT_DIR),
        ],
        cwd=PROJECT_DIR,
        env=env,
    )
    context.log.info(run.stdout)
    if run.returncode != 0:
        raise Failure(description="dbt run failed")

    context.log.info("Running dbt tests...")
    test = run_cmd(
        [
            "dbt",
            "test",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(PROJECT_DIR),
        ],
        cwd=PROJECT_DIR,
        env=env,
    )
    context.log.info(test.stdout)
    if test.returncode != 0:
        raise Failure(description="dbt test failed")
    context.log.info("dbt run & test completed.")


@op(description="Run YOLO detection and load results into Postgres, then refresh image marts")
def run_yolo_enrichment(context):
    context.log.info("Running YOLO detections...")
    detect = run_cmd([sys.executable, str(ROOT / "src" / "yolo_detect.py")], cwd=ROOT)
    context.log.info(detect.stdout)
    if detect.returncode != 0:
        raise Failure(description="YOLO detection failed")

    context.log.info("Loading YOLO detections into Postgres...")
    load = run_cmd([sys.executable, str(ROOT / "src" / "load_yolo_to_postgres.py")], cwd=ROOT)
    context.log.info(load.stdout)
    if load.returncode != 0:
        raise Failure(description="Load YOLO detections failed")

    # Refresh the image detection fact after enrichment
    context.log.info("Refreshing fct_image_detections via dbt...")
    refresh = run_cmd(
        [
            "dbt",
            "run",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(PROJECT_DIR),
            "--select",
            "fct_image_detections",
        ],
        cwd=PROJECT_DIR,
    )
    context.log.info(refresh.stdout)
    if refresh.returncode != 0:
        raise Failure(description="dbt refresh of fct_image_detections failed")
    context.log.info("YOLO enrichment completed.")


@job
def daily_pipeline_job():
    # Execution order: scrape -> load -> dbt -> yolo (then selective dbt refresh)
    run_yolo_enrichment(run_dbt_transformations(load_raw_to_postgres(scrape_telegram_data())))


daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 2 * * *",  # daily at 02:00 UTC
    execution_timezone="UTC",
)


# Simple failure hook to surface errors (can be extended to send alerts)
@daily_pipeline_job.failure_hook
def on_failure(context: HookContext):
    context.log.error(f"Pipeline failed for run_id={context.run_id}")


definitions = Definitions(jobs=[daily_pipeline_job], schedules=[daily_schedule])
