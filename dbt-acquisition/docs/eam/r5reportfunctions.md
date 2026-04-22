# r5reportfunctions

eam_R5REPORTFUNCTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reportfunctions` |
| **Materialization** | `incremental` |
| **Primary keys** | `rfn_function` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rfn_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RFN_LASTSAVED |
| `rfn_fieldorder` | `string` | `varchar` |  |  | RFN_FIELDORDER |
| `rfn_groupby` | `string` | `varchar` |  |  | RFN_GROUPBY |
| `rfn_orderby` | `string` | `varchar` |  |  | RFN_ORDERBY |
| `rfn_ordertype` | `string` | `varchar` |  |  | RFN_ORDERTYPE |
| `rfn_whereclause1` | `string` | `varchar` |  |  | RFN_WHERECLAUSE1 |
| `rfn_whereclause2` | `string` | `varchar` |  |  | RFN_WHERECLAUSE2 |
| `rfn_updatecount` | `float` | `float` |  |  | RFN_UPDATECOUNT |
| `rfn_updated` | `timestamp` | `timestamp_ntz` |  |  | RFN_UPDATED |
| `rfn_function` | `string` | `varchar` | `PK` | `unique` | RFN_FUNCTION |
| `rfn_fromclause` | `string` | `varchar` |  |  | RFN_FROMCLAUSE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
