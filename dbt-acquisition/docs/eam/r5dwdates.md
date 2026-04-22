# r5dwdates

eam_R5DWDATES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwdates` |
| **Materialization** | `incremental` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zdt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZDT_LASTSAVED |
| `zdt_date` | `timestamp` | `timestamp_ntz` |  |  | ZDT_DATE |
| `zdt_runningdaynum` | `float` | `float` |  |  | ZDT_RUNNINGDAYNUM |
| `zdt_runningweeknum` | `float` | `float` |  |  | ZDT_RUNNINGWEEKNUM |
| `zdt_runningmonthnum` | `float` | `float` |  |  | ZDT_RUNNINGMONTHNUM |
| `zdt_daynuminmonth` | `float` | `float` |  |  | ZDT_DAYNUMINMONTH |
| `zdt_daynuminyear` | `float` | `float` |  |  | ZDT_DAYNUMINYEAR |
| `zdt_weeknuminyear` | `float` | `float` |  |  | ZDT_WEEKNUMINYEAR |
| `zdt_monthnuminyear` | `float` | `float` |  |  | ZDT_MONTHNUMINYEAR |
| `zdt_yearmonth` | `string` | `varchar` |  |  | ZDT_YEARMONTH |
| `zdt_quarter` | `float` | `float` |  |  | ZDT_QUARTER |
| `zdt_yearquarter` | `string` | `varchar` |  |  | ZDT_YEARQUARTER |
| `zdt_halfyear` | `float` | `float` |  |  | ZDT_HALFYEAR |
| `zdt_year` | `float` | `float` |  |  | ZDT_YEAR |
| `zdt_lastdayinmonth` | `string` | `varchar` |  |  | ZDT_LASTDAYINMONTH |
| `zdt_key` | `float` | `float` |  |  | ZDT_KEY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
