# r5mobilequeries

eam_R5MOBILEQUERIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobilequeries` |
| **Materialization** | `incremental` |
| **Primary keys** | `mqr_gridname`, `mqr_tablename`, `mqr_version` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mqr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MQR_LASTSAVED |
| `mqr_version` | `string` | `varchar` | `PK` |  | MQR_VERSION |
| `mqr_tablename` | `string` | `varchar` | `PK` |  | MQR_TABLENAME |
| `mqr_createtable` | `string` | `varchar` |  |  | MQR_CREATETABLE. Max length: 0 |
| `mqr_sqltext` | `string` | `varchar` |  |  | MQR_SQLTEXT. Max length: 0 |
| `mqr_preparegrid` | `string` | `varchar` |  |  | MQR_PREPAREGRID |
| `mqr_columnmap` | `string` | `varchar` |  |  | MQR_COLUMNMAP |
| `mqr_singlethreadperconn` | `string` | `varchar` |  |  | MQR_SINGLETHREADPERCONN |
| `mqr_preparetableused` | `string` | `varchar` |  |  | MQR_PREPARETABLEUSED |
| `mqr_updatecount` | `float` | `float` |  |  | MQR_UPDATECOUNT |
| `mqr_gridname` | `string` | `varchar` | `PK` |  | MQR_GRIDNAME |
| `mqr_updated` | `timestamp` | `timestamp_ntz` |  |  | MQR_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
