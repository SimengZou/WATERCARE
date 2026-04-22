# ttaad111

LN ttaad111 Data Languages table, Generated 2026-04-10T19:42:38Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ttaad111` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `dlan`, `icnt` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `dlan` | `string` | `varchar` | `PK` | `not_null` | (required) ISO 639 Language. Max length: 3 |
| `icnt` | `string` | `varchar` | `PK` | `not_null` | (required) ISO 3166-1 Country. Max length: 2 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: ttaad.dlst.base, ttaad.dlst.available, ttaad.dlst.added, ttaad.dlst.deleted, ttaad.dlst.pending.add, ttaad.dlst.pending.delete, ttaad.dlst.migrated |
| `icnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad113 ISO 3166-1 Countries |
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
