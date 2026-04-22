# r5userdefinedfieldlovvals

eam_R5USERDEFINEDFIELDLOVVALS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userdefinedfieldlovvals` |
| **Materialization** | `incremental` |
| **Primary keys** | `udl_rentity`, `udl_field`, `udl_code` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `udl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UDL_LASTSAVED |
| `udl_field` | `string` | `varchar` | `PK` |  | UDL_FIELD |
| `udl_code` | `string` | `varchar` | `PK` |  | UDL_CODE |
| `udl_updated` | `timestamp` | `timestamp_ntz` |  |  | UDL_UPDATED |
| `udl_updatecount` | `float` | `float` |  |  | UDL_UPDATECOUNT |
| `udl_notused` | `string` | `varchar` |  |  | UDL_NOTUSED |
| `udl_rentity` | `string` | `varchar` | `PK` |  | UDL_RENTITY |
| `udl_desc` | `string` | `varchar` |  |  | UDL_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
