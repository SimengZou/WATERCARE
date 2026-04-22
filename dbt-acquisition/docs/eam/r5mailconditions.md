# r5mailconditions

eam_R5MAILCONDITIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mailconditions` |
| **Materialization** | `incremental` |
| **Primary keys** | `mac_pk` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mac_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MAC_LASTSAVED |
| `mac_template` | `string` | `varchar` |  |  | MAC_TEMPLATE |
| `mac_column` | `string` | `varchar` |  |  | MAC_COLUMN |
| `mac_criteria` | `string` | `varchar` |  |  | MAC_CRITERIA |
| `mac_value1` | `string` | `varchar` |  |  | MAC_VALUE1 |
| `mac_updatecount` | `float` | `float` |  |  | MAC_UPDATECOUNT |
| `mac_columngro` | `float` | `float` |  |  | MAC_COLUMNGRO |
| `mac_andor` | `string` | `varchar` |  |  | MAC_ANDOR |
| `mac_attribpk` | `string` | `varchar` |  |  | MAC_ATTRIBPK |
| `mac_pk` | `string` | `varchar` | `PK` | `unique` | MAC_PK |
| `mac_table` | `string` | `varchar` |  |  | MAC_TABLE |
| `mac_value2` | `string` | `varchar` |  |  | MAC_VALUE2 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
