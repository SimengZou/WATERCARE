# r5alerthistory

eam_R5ALERTHISTORY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alerthistory` |
| **Materialization** | `incremental` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `alh_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ALH_LASTSAVED |
| `alh_rstatus` | `string` | `varchar` |  |  | ALH_RSTATUS |
| `alh_rtype` | `string` | `varchar` |  |  | ALH_RTYPE |
| `alh_entitycode` | `string` | `varchar` |  |  | ALH_ENTITYCODE |
| `alh_entityorg` | `string` | `varchar` |  |  | ALH_ENTITYORG |
| `alh_resultcode` | `string` | `varchar` |  |  | ALH_RESULTCODE |
| `alh_resultorg` | `string` | `varchar` |  |  | ALH_RESULTORG |
| `alh_errormessage` | `string` | `varchar` |  |  | ALH_ERRORMESSAGE |
| `alh_created` | `timestamp` | `timestamp_ntz` |  |  | ALH_CREATED |
| `alh_updatecount` | `float` | `float` |  |  | ALH_UPDATECOUNT |
| `alh_alert` | `string` | `varchar` |  |  | ALH_ALERT |
| `alh_template` | `string` | `varchar` |  |  | ALH_TEMPLATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
