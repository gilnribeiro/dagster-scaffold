## Getting started

First install the `requirements.txt` using:
```bash
pip install -r requirements.txt
```

Then, navigate to `my-dagster-project/` your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `my_dagster_project/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

### Adjusting Contants and .Env variables
Adjust the paths for the variables at `my-dagster-project\my_dagster_project\constants.py`, and at `my-dagster-project\.env`

## How it works:
- Drop the csv files on SFTP and the sensor will check every 30s if there are new files, if so, its starts the transformation process and logs the progress on the duckdb workflow table.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `my_dagster_project_tests` directory and you can run tests using `pytest`:

```bash
pytest my_dagster_project_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
