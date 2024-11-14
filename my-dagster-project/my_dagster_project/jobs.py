from dagster import AssetSelection, define_asset_job

ato_assets = AssetSelection.all()

ato_file_processing_job = define_asset_job(
    name="ato_file_processing_job", selection=ato_assets
)
