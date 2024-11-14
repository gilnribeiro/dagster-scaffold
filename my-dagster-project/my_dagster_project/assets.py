from dagster import AssetExecutionContext, asset, Config, materialize
from my_dagster_project.resources import ApiResource, WorkflowLoggerResource
import os
import shutil
from my_dagster_project.constants import METADATA_TABLE, OUTPUT_DIR, INPUT_DIR
import polars as pl
from my_dagster_project.utils import log_workflow


class FileConfig(Config):
    source: str
    source_path: str


@asset()
@log_workflow(
    task_name="Load Country Files",
    description="Get files from SFTP and move them to Input.",
)
def country_files(
    context: AssetExecutionContext,
    api: ApiResource,
    workflow_logger: WorkflowLoggerResource,
) -> None:
    # Ensure source and destination folders exist
    if not os.path.exists(api.source_folder):
        print(f"Source folder {api.source_folder} does not exist.")
        return

    if not os.path.exists(api.destination_folder):
        os.makedirs(
            api.destination_folder
        )  # Create destination folder if it doesn't exist

    # Move each file from the source to the destination
    for filename in os.listdir(api.source_folder):
        source_path = os.path.join(api.source_folder, filename)
        destination_path = os.path.join(api.destination_folder, filename)

        if os.path.isfile(source_path):  # Only move files, not directories
            shutil.move(source_path, destination_path)
            print(f"Moved: {filename}")
        else:
            print(f"Skipping: {filename} (not a file)")


@asset(
    deps=[country_files],
    # io_manager_key="polars_parquet_io_manager",
    io_manager_key="io_manager",
    # key_prefix="harmonization_bmi",
    required_resource_keys={"workflow_logger"},
)
@log_workflow(task_name="Perform BMI harmonization", description="Harmonize BMI data.")
def harmonization_bmi(
    context: AssetExecutionContext, config: FileConfig
) -> pl.DataFrame:
    context.log.info("Added BMI column")
    # Transformation: Adding BMI (Body Mass Index) column
    return pl.read_csv(f"{INPUT_DIR}/{config.source}").with_columns(
        (pl.col("weight_kg") / (pl.col("height_cm") / 100) ** 2).alias("bmi")
    )


@asset(
    # io_manager_key="polars_parquet_io_manager",
    io_manager_key="io_manager",
    # key_prefix="harmonization_age",
    required_resource_keys={"workflow_logger"},
)
@log_workflow(task_name="Perform AGE harmonization", description="Harmonize AGE data.")
def harmonization_age(
    context: AssetExecutionContext,
    harmonization_bmi: pl.DataFrame,
    metadata_hight: pl.DataFrame,
) -> pl.DataFrame:
    # Transformation: Adding a category for age groups
    context.log.info("Added Age Group column")
    return harmonization_bmi.with_columns(
        pl.when(pl.col(["age"]) < 30)
        .then(pl.lit("Young"))
        .when(pl.col(["age"]) < 60)
        .then(pl.lit("Middle-aged"))
        .otherwise(pl.lit("Senior"))
        .alias("age_group")
    )


@asset(io_manager_key="io_manager")
def metadata_hight(context: AssetExecutionContext) -> pl.DataFrame:
    context.log.info("Loaded Metadata table 'hight_metadata'.")
    return pl.read_excel(METADATA_TABLE)


@asset(required_resource_keys={"workflow_logger"})
@log_workflow(task_name="Export Data", description="Export data to csv output")
def transformed_table(
    context: AssetExecutionContext,
    harmonization_age: pl.DataFrame,
    metadata_hight: pl.DataFrame,
    config: FileConfig,
) -> None:
    # Transformation: Adding a category for age groups
    context.log.info("Exported data to output/")
    harmonization_age.write_csv(f"{OUTPUT_DIR}/transformed_{config.source}")


if __name__ == "__main__":
    materialize(
        [country_files],
        # run_config={"context": AssetExecutionContext},
        resources={
            "api": ApiResource(),
            "workflow_logger": WorkflowLoggerResource(stop_on_failure=True),
        },
    )
