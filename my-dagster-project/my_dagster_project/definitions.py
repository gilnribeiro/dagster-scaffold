from dagster import Definitions, load_assets_from_modules

# from dagster_polars import PolarsParquetIOManager
from my_dagster_project.resources import ApiResource, WorkflowLoggerResource
from my_dagster_project import assets
from my_dagster_project.sensors import sftp_sensor_with_skip_reasons
from dagster_duckdb_polars import DuckDBPolarsIOManager

# from dagster_duckdb_polars import DuckDBPolarssIOManager

all_assets = load_assets_from_modules([assets])
all_sensors = [sftp_sensor_with_skip_reasons]

defs = Definitions(
    assets=[*all_assets],
    resources={
        "api": ApiResource(),
        "workflow_logger": WorkflowLoggerResource(stop_on_failure=True),
        "io_manager": DuckDBPolarsIOManager(
            database="C:/Users/gilnr/OneDrive/Ambiente de Trabalho/ITC Contract/GitHub/Dagster_Scaffold/my-dagster-project/io_database/local_duckdb_database.duckdb",
            schema="DBO",
        ),
        # "polars_parquet_io_manager": PolarsParquetIOManager(
        #     base_dir="C:/Users/gilnr/OneDrive/Ambiente de Trabalho/ITC Contract/GitHub/Dagster_Scaffold/my-dagster-project/io_database"
        # ),
    },
    sensors=all_sensors,
)
