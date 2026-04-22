# r5rcodes

eam_R5RCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5rcodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `rco_rentity`, `rco_rcode` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rco_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RCO_LASTSAVED |
| `rco_rcode` | `string` | `varchar` | `PK` |  | RCO_RCODE |
| `rco_updatecount` | `float` | `float` |  |  | RCO_UPDATECOUNT |
| `rco_textcode` | `string` | `varchar` |  |  | RCO_TEXTCODE |
| `rco_rentity` | `string` | `varchar` | `PK` |  | RCO_RENTITY |
| `rco_desc` | `string` | `varchar` |  |  | RCO_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
