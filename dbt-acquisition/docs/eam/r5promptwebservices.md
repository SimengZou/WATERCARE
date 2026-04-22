# r5promptwebservices

eam_R5PROMPTWEBSERVICES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5promptwebservices` |
| **Materialization** | `incremental` |
| **Primary keys** | `pws_processgroup`, `pws_wsprmcode` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pws_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PWS_LASTSAVED |
| `pws_function` | `string` | `varchar` |  |  | PWS_FUNCTION |
| `pws_tab` | `string` | `varchar` |  |  | PWS_TAB |
| `pws_actioncode` | `string` | `varchar` |  |  | PWS_ACTIONCODE |
| `pws_webservice` | `string` | `varchar` |  |  | PWS_WEBSERVICE |
| `pws_orgxpath` | `string` | `varchar` |  |  | PWS_ORGXPATH |
| `pws_updated` | `timestamp` | `timestamp_ntz` |  |  | PWS_UPDATED |
| `pws_updatecount` | `float` | `float` |  |  | PWS_UPDATECOUNT |
| `pws_wstitle` | `string` | `varchar` |  |  | PWS_WSTITLE |
| `pws_cfkeycode` | `string` | `varchar` |  |  | PWS_CFKEYCODE |
| `pws_topblocktitle` | `string` | `varchar` |  |  | PWS_TOPBLOCKTITLE |
| `pws_bottomblocktitle` | `string` | `varchar` |  |  | PWS_BOTTOMBLOCKTITLE |
| `pws_cfblocktitle` | `string` | `varchar` |  |  | PWS_CFBLOCKTITLE |
| `pws_processgroup` | `float` | `float` | `PK` |  | PWS_PROCESSGROUP |
| `pws_wsprmcode` | `string` | `varchar` | `PK` |  | PWS_WSPRMCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
