# whinh435

LN whinh435 Delivery Notes table, Generated 2026-04-10T19:42:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh435` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `prdn` |
| **Column count** | 71 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `prdn` | `string` | `varchar` | `PK` | `not_null` | (required) Preliminary Delivery Note. Max length: 9 |
| `deln` | `string` | `varchar` |  |  | Delivery Note. Max length: 19 |
| `fsyr` | `integer` | `int` |  |  | Year |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `sfty` | `integer` | `int` |  |  | Ship-from Type. Allowed values: 1, 2, 3, 4, 10 |
| `sfty_kw` | `string` | `varchar` |  |  | Ship-from Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `sfco` | `string` | `varchar` |  |  | Ship-from Code. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcp` | `integer` | `int` |  |  | Ship-from Company |
| `stty` | `integer` | `int` |  |  | Ship-to Type. Allowed values: 1, 2, 3, 4, 10 |
| `stty_kw` | `string` | `varchar` |  |  | Ship-to Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `stco` | `string` | `varchar` |  |  | Ship-to Code. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcp` | `integer` | `int` |  |  | Ship-to Company |
| `tsad` | `string` | `varchar` |  |  | Transit Address. Max length: 9 |
| `depc` | `integer` | `int` |  |  | Office Company |
| `wdep` | `string` | `varchar` |  |  | Office. Max length: 6 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `dnst` | `integer` | `int` |  |  | Delivery Note Status. Allowed values: 1, 2, 3, 4, 5 |
| `dnst_kw` | `string` | `varchar` |  |  | Delivery Note Status (keyword). Allowed values: whinh.dnst.canceled, whinh.dnst.open, whinh.dnst.frozen, whinh.dnst.confirmed, whinh.dnst.completed |
| `finp` | `integer` | `int` |  |  | Final Print. Allowed values: 1, 2 |
| `finp_kw` | `string` | `varchar` |  |  | Final Print (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Printing Date |
| `dcdt` | `timestamp` | `timestamp_ntz` |  |  | Document Date |
| `manl` | `integer` | `int` |  |  | Manual. Allowed values: 1, 2 |
| `manl_kw` | `string` | `varchar` |  |  | Manual (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `taxs` | `string` | `varchar` |  |  | Own Identification Number. Max length: 35 |
| `taxp` | `string` | `varchar` |  |  | Business Partner Identification Number. Max length: 35 |
| `qpac` | `float` | `float` |  |  | Number of Packages |
| `shap__en_us` | `string` | `varchar` |  | `not_null` | (required) Shape - base language. Max length: 50 |
| `nota__en_us` | `string` | `varchar` |  | `not_null` | (required) Note A - base language. Max length: 50 |
| `notb__en_us` | `string` | `varchar` |  | `not_null` | (required) Note B - base language. Max length: 50 |
| `notc__en_us` | `string` | `varchar` |  | `not_null` | (required) Note C - base language. Max length: 50 |
| `grwt` | `float` | `float` |  |  | Gross Weight |
| `cwun` | `string` | `varchar` |  |  | Unit of Measure. Max length: 3 |
| `carp` | `string` | `varchar` |  |  | Pick-up Carrier/LSP. Max length: 3 |
| `fnsr` | `string` | `varchar` |  |  | Series for Delivery Note. Max length: 8 |
| `load_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh440 Loads |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `tsad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `depc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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
