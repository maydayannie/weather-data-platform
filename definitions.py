# definitions.py
import os
from pathlib import Path
from dagster import (
    AssetExecutionContext, 
    asset, 
    Definitions, 
    define_asset_job, 
    ScheduleDefinition
)
from dagster_dbt import DbtCliResource, dbt_assets
import subprocess

# 1. define dbt project directory
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "dbt_weather").resolve()

# 2. 
@asset(compute_kind="python", description="從 OpenWeather 抓取天氣與空污資料並寫入 DuckDB")
def raw_weather_and_pollution_api():
    script_path = Path(__file__).parent / "fetch_weather.py"
    result = subprocess.run(["python", str(script_path)], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"API 抓取失敗: {result.stderr}")
    return "落地成功"

# 3. Auto read dbt models from manifest.json and create assets
@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def dbt_weather_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

# Define a auto job that will execute all assets（Python + dbt） in this project
weather_platform_job = define_asset_job(
    name="weather_platform_job",
    selection="*" 
)

# Define schedule to trigger the job every day
weather_daily_9am_schedule = ScheduleDefinition(
    name="weather_daily_9am_schedule",
    job=weather_platform_job,
    cron_schedule="0 9 * * *",  
    execution_timezone="Asia/Taipei"  
)

# 4. Register assets, jobs, schedules, and resources in Dagster
defs = Definitions(
    assets=[raw_weather_and_pollution_api, dbt_weather_models],
    jobs=[weather_platform_job],         # Register jobs 
    schedules=[weather_daily_9am_schedule], # Register schedules
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR)),
    },
)