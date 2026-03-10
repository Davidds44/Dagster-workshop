import dagster as dg
from dagster_essentials.defs.assets import constants

startdate = constants.START_DATE
enddate = constants.END_DATE


monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=startdate,
    end_date=enddate
)

weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date=startdate,
    end_date=enddate
)
