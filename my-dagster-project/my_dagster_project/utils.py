from functools import wraps
from dagster import AssetExecutionContext


def log_workflow(task_name: str, description: str):
    """Decorator to log workflow status using WorkflowLoggerResource."""

    def decorator(asset_fn):
        @wraps(asset_fn)
        def wrapper(context: AssetExecutionContext, *args, **kwargs):
            # Access the workflow_logger resource from context
            print("Enter Workflow Logger")
            try:
                # Execute the original asset function
                result = asset_fn(context, *args, **kwargs)

                # Log success if asset function completes without exception
                context.resources.workflow_logger.handle_success(task_name, description)
                return result
            except Exception as e:
                # Log failure if an exception is raised
                context.resources.workflow_logger.handle_failure(
                    task_name, description, str(e), context
                )
                raise  # Re-raise the exception after logging

        return wrapper

    return decorator
