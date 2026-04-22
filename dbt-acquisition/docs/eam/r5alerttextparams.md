# r5alerttextparams

eam_R5ALERTTEXTPARAMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alerttextparams` |
| **Materialization** | `incremental` |
| **Primary keys** | `atp_alert`, `atp_rtype`, `atp_template`, `atp_parameter` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `atp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ATP_LASTSAVED |
| `atp_rtype` | `string` | `varchar` | `PK` |  | ATP_RTYPE |
| `atp_template` | `string` | `varchar` | `PK` |  | ATP_TEMPLATE |
| `atp_parameter` | `float` | `float` | `PK` |  | ATP_PARAMETER |
| `atp_value` | `string` | `varchar` |  |  | ATP_VALUE |
| `atp_includerecipient` | `string` | `varchar` |  |  | ATP_INCLUDERECIPIENT |
| `atp_updatecount` | `float` | `float` |  |  | ATP_UPDATECOUNT |
| `atp_reportparameter` | `float` | `float` |  |  | ATP_REPORTPARAMETER |
| `atp_alert` | `string` | `varchar` | `PK` |  | ATP_ALERT |
| `atp_fieldid` | `float` | `float` |  |  | ATP_FIELDID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
