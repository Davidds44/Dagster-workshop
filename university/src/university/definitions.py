from pathlib import Path

import dagster as dg

from university.defs.resources import database


@dg.definitions
def defs():
    base = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    return dg.Definitions.merge(
        base,
        dg.Definitions(resources={"database": database}),
    )
