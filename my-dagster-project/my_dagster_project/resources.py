from pydantic import Field
from dagster import AssetExecutionContext, ConfigurableResource
from dagster_duckdb import DuckDBResource
from my_dagster_project.constants import DUCKDB_DATABASE, INPUT_DIR, SFTP_DIR


class ApiResource(ConfigurableResource):
    source_folder: str = Field(
        default=SFTP_DIR,
        description="Source folder path",
    )
    destination_folder: str = Field(
        default=INPUT_DIR,
        description="Destination folder path",
    )


# class CSVResource(ConfigurableResource):
#     location: str

#     def load_database(self):
#         return pl.read_csv(self.location)


class WorkflowLoggerResource(ConfigurableResource):
    stop_on_failure: bool  # Configurable option to stop on failure or not

    def log(self, task_name: str, description: str, status: str, comment: str = ""):
        # Ensure the table is created if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS WorkflowDetails (
            task TEXT,
            description TEXT,
            status TEXT,
            comment TEXT
        );
        INSERT INTO WorkflowDetails (task, description, status, comment) 
        VALUES (?, ?, ?, ?);
        """

        # Use the DuckDB connection to create the table and insert the data
        with DuckDBResource(
            database=DUCKDB_DATABASE,
        ).get_connection() as conn:
            # Create table if it doesn't exist
            # Insert the provided task details into the WorkflowDetails table
            conn.execute(create_table_query, (task_name, description, status, comment))

    def handle_failure(
        self,
        task_name: str,
        description: str,
        error_message: str,
        context: AssetExecutionContext,
    ):
        self.log(task_name, description, "Failure", error_message)
        if self.stop_on_failure:
            raise Exception(f"Pipeline stopping due to failure: {error_message}")
        else:
            context.log.error(f"Error logged: {error_message}")

    def handle_success(self, task_name: str, description: str):
        self.log(task_name, description, "Success", "")
