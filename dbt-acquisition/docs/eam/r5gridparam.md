# r5gridparam

eam_R5GRIDPARAM

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5gridparam` |
| **Materialization** | `incremental` |
| **Primary keys** | `gdp_gridid`, `gdp_param` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `gdp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | GDP_LASTSAVED |
| `gdp_param` | `string` | `varchar` | `PK` |  | GDP_PARAM |
| `gdp_tagname` | `string` | `varchar` |  |  | GDP_TAGNAME |
| `gdp_updatecount` | `float` | `float` |  |  | GDP_UPDATECOUNT |
| `gdp_updated` | `timestamp` | `timestamp_ntz` |  |  | GDP_UPDATED |
| `gdp_gridid` | `float` | `float` | `PK` |  | GDP_GRIDID |
| `gdp_datatype` | `string` | `varchar` |  |  | GDP_DATATYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
