# tpptc510

LN tpptc510 Budget Amounts of Activities by Version table, Generated 2026-04-10T19:42:27Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc510` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `vers`, `cact`, `rgdt` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `vers` | `integer` | `int` | `PK` | `not_null` | (required) Version |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `rgdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Registration Date |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `ambu` | `float` | `float` |  |  | Budget Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `amit` | `float` | `float` |  |  | Material Amount |
| `amts` | `float` | `float` |  |  | Labor Amount |
| `ameq` | `float` | `float` |  |  | Equipment Amount |
| `amsb` | `float` | `float` |  |  | Subcontracting Amount |
| `amsn` | `float` | `float` |  |  | Sundry Cost Amount |
| `stat` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2 |
| `stat_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tpptc.stch.free, tpptc.stch.definite |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_vers_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc500 Budget Versions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
