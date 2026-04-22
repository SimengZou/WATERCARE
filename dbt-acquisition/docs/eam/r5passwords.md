# r5passwords

eam_R5PASSWORDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5passwords` |
| **Materialization** | `incremental` |
| **Primary keys** | `pwd_user`, `pwd_password`, `pwd_lastused` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pwd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PWD_LASTSAVED |
| `pwd_password` | `string` | `varchar` | `PK` |  | PWD_PASSWORD |
| `pwd_updatecount` | `float` | `float` |  |  | PWD_UPDATECOUNT |
| `pwd_user` | `string` | `varchar` | `PK` |  | PWD_USER |
| `pwd_lastused` | `timestamp` | `timestamp_ntz` | `PK` |  | PWD_LASTUSED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
