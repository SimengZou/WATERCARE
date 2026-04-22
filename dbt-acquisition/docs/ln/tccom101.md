# tccom101

LN tccom101 Business Partner Defaults table, Generated 2026-04-10T19:41:01Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom101` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `sern` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Number |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ctyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cfir` | `integer` | `int` |  |  | Financial Roles. Allowed values: 1, 2 |
| `cfir_kw` | `string` | `varchar` |  |  | Financial Roles (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cinm` | `string` | `varchar` |  |  | Invoicing Method. Max length: 3 |
| `crat` | `string` | `varchar` |  |  | Credit Rating. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `pcur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cfcg` | `string` | `varchar` |  |  | Financial Customer Group. Max length: 3 |
| `scur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `styp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `sfir` | `integer` | `int` |  |  | Financial Roles. Allowed values: 1, 2 |
| `sfir_kw` | `string` | `varchar` |  |  | Financial Roles (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `cfsg` | `string` | `varchar` |  |  | Financial Supplier Group. Max length: 3 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `btbv` | `integer` | `int` |  |  | To be Verified. Allowed values: 1, 2 |
| `btbv_kw` | `string` | `varchar` |  |  | To be Verified (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bprl` | `integer` | `int` |  |  | Business Partner Role. Allowed values: 1, 2, 3, 4 |
| `bprl_kw` | `string` | `varchar` |  |  | Business Partner Role (keyword). Allowed values: tcbprl.none, tcbprl.customer, tcbprl.supplier, tcbprl.both |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ctyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cinm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs055 Invoicing Methods |
| `crat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs064 Credit Ratings |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `pcur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `scur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `spay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
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
