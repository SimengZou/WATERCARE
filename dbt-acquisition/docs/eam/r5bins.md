# r5bins

eam_R5BINS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bins` |
| **Materialization** | `incremental` |
| **Primary keys** | `bin_store`, `bin_code` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `bin_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BIN_LASTSAVED |
| `bin_code` | `string` | `varchar` | `PK` |  | BIN_CODE |
| `bin_desc` | `string` | `varchar` |  |  | BIN_DESC |
| `bin_created` | `timestamp` | `timestamp_ntz` |  |  | BIN_CREATED |
| `bin_createdby` | `string` | `varchar` |  |  | BIN_CREATEDBY |
| `bin_updatedby` | `string` | `varchar` |  |  | BIN_UPDATEDBY |
| `bin_notused` | `string` | `varchar` |  |  | BIN_NOTUSED |
| `bin_updatecount` | `float` | `float` |  |  | BIN_UPDATECOUNT |
| `bin_forstock` | `string` | `varchar` |  |  | BIN_FORSTOCK |
| `bin_forhelditems` | `string` | `varchar` |  |  | BIN_FORHELDITEMS |
| `bin_store` | `string` | `varchar` | `PK` |  | BIN_STORE |
| `bin_updated` | `timestamp` | `timestamp_ntz` |  |  | BIN_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
