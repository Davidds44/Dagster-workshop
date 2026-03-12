from pathlib import Path

from dagster_dbt import DbtProject


dbt_project = DbtProject(
    # This file lives at: src/university/defs/project.py
    # The dbt project lives at: <project_root>/analytics
    project_dir=Path(__file__).resolve().parents[3] / "analytics",
    profiles_dir=Path(__file__).resolve().parents[3] / "analytics",
)

dbt_project.prepare_if_dev()

