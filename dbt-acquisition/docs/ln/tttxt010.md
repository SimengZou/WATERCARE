# tttxt010

LN Texts Texts table, Generated 2026-04-10T19:42:40Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tttxt010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ctxt`, `clan` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ctxt` | `integer` | `int` | `PK` | `not_null` | (required) Text Number |
| `clan` | `string` | `varchar` | `PK` | `not_null` | (required) Language. Allowed values: nl_NL, en_US, de_DE, fr_FR, es_ES, it_IT, sv_SE, hr_HR, pt_PT, ru_RU, th_TH, sr_BA, ar_SA, bg_BG, pl_PL, fi_FI, he_IL, ja_JP, ko_KR, hu_HU, zh_TW, zh_CN, pt_BR, ro_RO, sl_SI, cs_CZ, uk_UA, vi_VN, sk_SK, en_GB, tr_TR |
| `text` | `string` | `varchar` |  |  | Text Line |
| `ctxt_clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt002 Language Dependent Text Data |
| `ctxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
