# r5editionucodes

eam_R5EDITIONUCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5editionucodes` |
| **Materialization** | `incremental` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `edu_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EDU_LASTSAVED |
| `edu_code` | `string` | `varchar` |  |  | EDU_CODE |
| `edu_lang` | `string` | `varchar` |  |  | EDU_LANG |
| `edu_rentity` | `string` | `varchar` |  |  | EDU_RENTITY |
| `edu_market` | `string` | `varchar` |  |  | EDU_MARKET |
| `edu_textcode` | `string` | `varchar` |  |  | EDU_TEXTCODE |
| `edu_rcode` | `string` | `varchar` |  |  | EDU_RCODE |
| `edu_desc` | `string` | `varchar` |  |  | EDU_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
