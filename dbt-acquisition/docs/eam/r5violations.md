# r5violations

eam_R5VIOLATIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5violations` |
| **Materialization** | `incremental` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `vio_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | VIO_LASTSAVED |
| `vio_password` | `string` | `varchar` |  |  | VIO_PASSWORD |
| `vio_date` | `timestamp` | `timestamp_ntz` |  |  | VIO_DATE |
| `vio_osmachine` | `string` | `varchar` |  |  | VIO_OSMACHINE |
| `vio_updatecount` | `float` | `float` |  |  | VIO_UPDATECOUNT |
| `vio_user` | `string` | `varchar` |  |  | VIO_USER |
| `vio_osuser` | `string` | `varchar` |  |  | VIO_OSUSER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
