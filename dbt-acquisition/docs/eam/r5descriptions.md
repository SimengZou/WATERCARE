# r5descriptions

eam_R5DESCRIPTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5descriptions` |
| **Materialization** | `incremental` |
| **Primary keys** | `des_rentity`, `des_code`, `des_org`, `des_lang`, `des_rtype` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `des_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DES_LASTSAVED |
| `des_rentity` | `string` | `varchar` | `PK` |  | DES_RENTITY |
| `des_type` | `string` | `varchar` |  |  | DES_TYPE |
| `des_rtype` | `string` | `varchar` | `PK` |  | DES_RTYPE |
| `des_code` | `string` | `varchar` | `PK` |  | DES_CODE |
| `des_text` | `string` | `varchar` |  |  | DES_TEXT |
| `des_trans` | `string` | `varchar` |  |  | DES_TRANS |
| `des_org` | `string` | `varchar` | `PK` |  | DES_ORG |
| `des_updatecount` | `float` | `float` |  |  | DES_UPDATECOUNT |
| `des_entity` | `string` | `varchar` |  |  | DES_ENTITY |
| `des_lang` | `string` | `varchar` | `PK` |  | DES_LANG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
