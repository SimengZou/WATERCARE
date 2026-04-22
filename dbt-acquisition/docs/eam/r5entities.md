# r5entities

eam_R5ENTITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5entities` |
| **Materialization** | `incremental` |
| **Primary keys** | `ent_rentity` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ent_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ENT_LASTSAVED |
| `ent_statent` | `string` | `varchar` |  |  | ENT_STATENT |
| `ent_typent` | `string` | `varchar` |  |  | ENT_TYPENT |
| `ent_multilang` | `string` | `varchar` |  |  | ENT_MULTILANG |
| `ent_classif` | `string` | `varchar` |  |  | ENT_CLASSIF |
| `ent_table` | `string` | `varchar` |  |  | ENT_TABLE |
| `ent_addattribs` | `string` | `varchar` |  |  | ENT_ADDATTRIBS |
| `ent_freetext` | `string` | `varchar` |  |  | ENT_FREETEXT |
| `ent_addresses` | `string` | `varchar` |  |  | ENT_ADDRESSES |
| `ent_documents` | `string` | `varchar` |  |  | ENT_DOCUMENTS |
| `ent_assparts` | `string` | `varchar` |  |  | ENT_ASSPARTS |
| `ent_asspermits` | `string` | `varchar` |  |  | ENT_ASSPERMITS |
| `ent_ftaudit` | `string` | `varchar` |  |  | ENT_FTAUDIT |
| `ent_caaudit` | `string` | `varchar` |  |  | ENT_CAAUDIT |
| `ent_erecord` | `string` | `varchar` |  |  | ENT_ERECORD |
| `ent_audits` | `string` | `varchar` |  |  | ENT_AUDITS |
| `ent_updatecount` | `float` | `float` |  |  | ENT_UPDATECOUNT |
| `ent_updated` | `timestamp` | `timestamp_ntz` |  |  | ENT_UPDATED |
| `ent_entity` | `string` | `varchar` |  |  | ENT_ENTITY |
| `ent_rentity` | `string` | `varchar` | `PK` | `unique` | ENT_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
