# r5localehotkeys

eam_R5LOCALEHOTKEYS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5localehotkeys` |
| **Materialization** | `incremental` |
| **Primary keys** | `lhk_locale`, `lhk_code` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lhk_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LHK_LASTSAVED |
| `lhk_code` | `string` | `varchar` | `PK` |  | LHK_CODE |
| `lhk_default` | `float` | `float` |  |  | LHK_DEFAULT |
| `lhk_desc` | `string` | `varchar` |  |  | LHK_DESC |
| `lhk_extra` | `string` | `varchar` |  |  | LHK_EXTRA |
| `lhk_updatecount` | `float` | `float` |  |  | LHK_UPDATECOUNT |
| `lhk_locale` | `string` | `varchar` | `PK` |  | LHK_LOCALE |
| `lhk_key` | `float` | `float` |  |  | LHK_KEY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
