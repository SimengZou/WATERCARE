# r5dwfactfields

eam_R5DWFACTFIELDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwfactfields` |
| **Materialization** | `incremental` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ffl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FFL_LASTSAVED |
| `ffl_dmttable` | `string` | `varchar` |  |  | FFL_DMTTABLE |
| `ffl_desc` | `string` | `varchar` |  |  | FFL_DESC |
| `ffl_botnumber` | `float` | `float` |  |  | FFL_BOTNUMBER |
| `ffl_fieldid` | `float` | `float` |  |  | FFL_FIELDID |
| `ffl_field` | `string` | `varchar` |  |  | FFL_FIELD |
| `ffl_additivity` | `string` | `varchar` |  |  | FFL_ADDITIVITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
