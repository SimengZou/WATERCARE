# r5dlvextmenulang

eam_R5DLVEXTMENULANG

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dlvextmenulang` |
| **Materialization** | `incremental` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `eml_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EML_LASTSAVED |
| `eml_trans` | `string` | `varchar` |  |  | EML_TRANS |
| `eml_text` | `string` | `varchar` |  |  | EML_TEXT |
| `eml_updatecount` | `float` | `float` |  |  | EML_UPDATECOUNT |
| `eml_extmenu` | `string` | `varchar` |  |  | EML_EXTMENU |
| `eml_lang` | `string` | `varchar` |  |  | EML_LANG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
