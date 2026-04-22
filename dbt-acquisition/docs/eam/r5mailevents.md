# r5mailevents

eam_R5MAILEVENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mailevents` |
| **Materialization** | `incremental` |
| **Primary keys** | `mae_code` |
| **Column count** | 38 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mae_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MAE_LASTSAVED |
| `mae_date` | `timestamp` | `timestamp_ntz` |  |  | MAE_DATE |
| `mae_send` | `string` | `varchar` |  |  | MAE_SEND |
| `mae_rstatus` | `string` | `varchar` |  |  | MAE_RSTATUS |
| `mae_error` | `string` | `varchar` |  |  | MAE_ERROR |
| `mae_param1` | `string` | `varchar` |  |  | MAE_PARAM1 |
| `mae_param2` | `string` | `varchar` |  |  | MAE_PARAM2 |
| `mae_param3` | `string` | `varchar` |  |  | MAE_PARAM3 |
| `mae_param4` | `string` | `varchar` |  |  | MAE_PARAM4 |
| `mae_param5` | `string` | `varchar` |  |  | MAE_PARAM5 |
| `mae_param6` | `string` | `varchar` |  |  | MAE_PARAM6 |
| `mae_param7` | `string` | `varchar` |  |  | MAE_PARAM7 |
| `mae_param8` | `string` | `varchar` |  |  | MAE_PARAM8 |
| `mae_param9` | `string` | `varchar` |  |  | MAE_PARAM9 |
| `mae_param10` | `string` | `varchar` |  |  | MAE_PARAM10 |
| `mae_param11` | `string` | `varchar` |  |  | MAE_PARAM11 |
| `mae_param12` | `string` | `varchar` |  |  | MAE_PARAM12 |
| `mae_param13` | `string` | `varchar` |  |  | MAE_PARAM13 |
| `mae_param14` | `string` | `varchar` |  |  | MAE_PARAM14 |
| `mae_param15` | `string` | `varchar` |  |  | MAE_PARAM15 |
| `mae_updatecount` | `float` | `float` |  |  | MAE_UPDATECOUNT |
| `mae_attribpk` | `string` | `varchar` |  |  | MAE_ATTRIBPK |
| `mae_ewsurl` | `string` | `varchar` |  |  | MAE_EWSURL |
| `mae_emailrecipient` | `string` | `varchar` |  |  | MAE_EMAILRECIPIENT |
| `mae_ptfsend` | `string` | `varchar` |  |  | MAE_PTFSEND |
| `mae_ptferror` | `string` | `varchar` |  |  | MAE_PTFERROR |
| `mae_docrentity` | `string` | `varchar` |  |  | MAE_DOCRENTITY |
| `mae_docpk` | `string` | `varchar` |  |  | MAE_DOCPK |
| `mae_emailerrcount` | `float` | `float` |  |  | MAE_EMAILERRCOUNT |
| `mae_ptferrcount` | `float` | `float` |  |  | MAE_PTFERRCOUNT |
| `mae_code` | `string` | `varchar` | `PK` | `unique` | MAE_CODE |
| `mae_template` | `string` | `varchar` |  |  | MAE_TEMPLATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
