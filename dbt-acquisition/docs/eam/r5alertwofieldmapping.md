# r5alertwofieldmapping

eam_R5ALERTWOFIELDMAPPING

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertwofieldmapping` |
| **Materialization** | `incremental` |
| **Primary keys** | `awf_alert`, `awf_wofield` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `awf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AWF_LASTSAVED |
| `awf_wofield` | `string` | `varchar` | `PK` |  | AWF_WOFIELD |
| `awf_gridfield` | `float` | `float` |  |  | AWF_GRIDFIELD |
| `awf_boilertextnumber` | `float` | `float` |  |  | AWF_BOILERTEXTNUMBER |
| `awf_updatecount` | `float` | `float` |  |  | AWF_UPDATECOUNT |
| `awf_alert` | `string` | `varchar` | `PK` |  | AWF_ALERT |
| `awf_gridfielddatatype` | `string` | `varchar` |  |  | AWF_GRIDFIELDDATATYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
