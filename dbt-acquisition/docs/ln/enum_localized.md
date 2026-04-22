# enum_localized

LN Enum_localized Translations of enum descriptions, Generated 2026-04-10T19:40:58Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_enum_localized` |
| **Materialization** | `incremental` |
| **Primary keys** | `keyword` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `keyword` | `string` | `varchar` | `PK` | `unique`, `not_null` | (required) Max length: 30 |
| `domain` | `string` | `varchar` |  | `not_null` | (required) Max length: 14 |
| `const` | `integer` | `int` |  | `not_null` | (required) |
| `description__nl_nl` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__en_us` | `string` | `varchar` |  | `not_null` | (required) base language. Max length: 255 |
| `description__de_de` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__fr_fr` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__es_es` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__it_it` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__sv_se` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__hr_hr` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__pt_pt` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__ru_ru` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__th_th` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__sr_ba` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__ar_sa` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__bg_bg` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__pl_pl` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__fi_fi` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__he_il` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__ja_jp` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__ko_kr` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__hu_hu` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__zh_tw` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__zh_cn` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__pt_br` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__ro_ro` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__sl_si` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__cs_cz` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__uk_ua` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__vi_vn` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__sk_sk` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__en_gb` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `description__tr_tr` | `string` | `varchar` |  |  | additional language. Max length: 255 |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
