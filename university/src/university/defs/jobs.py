from __future__ import annotations

import dagster as dg


raw_data_daily_job = dg.define_asset_job(
    name="raw_data_daily_job",
    selection=dg.AssetSelection.groups("raw_data"),
)

