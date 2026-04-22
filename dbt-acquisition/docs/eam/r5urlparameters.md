# r5urlparameters

eam_R5URLPARAMETERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5urlparameters` |
| **Materialization** | `incremental` |
| **Primary keys** | `url_userfunction`, `url_tab`, `url_parametername` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `url_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | URL_LASTSAVED |
| `url_tab` | `string` | `varchar` | `PK` |  | URL_TAB |
| `url_parametername` | `string` | `varchar` | `PK` |  | URL_PARAMETERNAME |
| `url_alternateparametername` | `string` | `varchar` |  |  | URL_ALTERNATEPARAMETERNAME |
| `url_system` | `string` | `varchar` |  |  | URL_SYSTEM |
| `url_active` | `string` | `varchar` |  |  | URL_ACTIVE |
| `url_updatecount` | `float` | `float` |  |  | URL_UPDATECOUNT |
| `url_usefieldvalue` | `string` | `varchar` |  |  | URL_USEFIELDVALUE |
| `url_userfunction` | `string` | `varchar` | `PK` |  | URL_USERFUNCTION |
| `url_parametervalue` | `string` | `varchar` |  |  | URL_PARAMETERVALUE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
