# r5dwstores

eam_R5DWSTORES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwstores` |
| **Materialization** | `incremental` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zso_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZSO_LASTSAVED |
| `zso_code` | `string` | `varchar` |  |  | ZSO_CODE |
| `zso_desc` | `string` | `varchar` |  |  | ZSO_DESC |
| `zso_locationdesc` | `string` | `varchar` |  |  | ZSO_LOCATIONDESC |
| `zso_classorg` | `string` | `varchar` |  |  | ZSO_CLASSORG |
| `zso_classdesc` | `string` | `varchar` |  |  | ZSO_CLASSDESC |
| `zso_locationcode` | `string` | `varchar` |  |  | ZSO_LOCATIONCODE |
| `zso_locationorg` | `string` | `varchar` |  |  | ZSO_LOCATIONORG |
| `zso_key` | `float` | `float` |  |  | ZSO_KEY |
| `zso_classcode` | `string` | `varchar` |  |  | ZSO_CLASSCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
