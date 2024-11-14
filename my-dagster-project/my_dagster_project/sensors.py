import os
from dagster import sensor, RunRequest, SkipReason
from .jobs import ato_file_processing_job
from .constants import SFTP_DIR


@sensor(job=ato_file_processing_job)
def sftp_sensor_with_skip_reasons():
    has_files = False
    for filename in os.listdir(SFTP_DIR):
        filepath = os.path.join(SFTP_DIR, filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config={
                    "ops": {
                        "harmonization_bmi": {
                            "config": {"source": filename, "source_path": filepath}
                        },
                        "transformed_table": {
                            "config": {"source": filename, "source_path": filepath}
                        },
                    }
                },
            )
            has_files = True
    if not has_files:
        yield SkipReason(f"No files found in {SFTP_DIR}.")
