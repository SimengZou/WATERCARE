# tipcf500

LN tipcf500 Product Variant IDs table, Generated 2026-04-10T19:41:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tipcf500` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpva` |
| **Column count** | 54 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpva` | `integer` | `int` | `PK` | `not_null` | (required) Product Variant |
| `cuid__en_us` | `string` | `varchar` |  | `not_null` | (required) configuration UID - base language. Max length: 40 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `reft` | `integer` | `int` |  |  | Reference Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `reft_kw` | `string` | `varchar` |  |  | Reference Type (keyword). Allowed values: tcreft.sls.quotation, tcreft.sls.order, tcreft.budget, tcreft.project, tcreft.standard, tcreft.tp, tcreft.psd.ord, tcreft.contract, tcreft.sls.rco |
| `refo` | `string` | `varchar` |  |  | Reference Order. Max length: 9 |
| `refp` | `integer` | `int` |  |  | Reference Position |
| `altn` | `integer` | `int` |  |  | Alternative Sales Quotation |
| `sost` | `integer` | `int` |  |  | Sales Options Status. Allowed values: 0, 12, 24, 36 |
| `sost_kw` | `string` | `varchar` |  |  | Sales Options Status (keyword). Allowed values: , tipcf.sost.not.present, tipcf.sost.to.be.processed, tipcf.sost.processed |
| `cuno` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `slpr` | `float` | `float` |  |  | Sales Price |
| `lnst` | `integer` | `int` |  |  | Status. Allowed values: 0, 5, 10, 15, 20, 25, 30, 35 |
| `lnst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: , tipcf.elns.unspecified, tipcf.elns.initial, tipcf.elns.configured, tipcf.elns.part.solved, tipcf.elns.solved, tipcf.elns.connection.err, tipcf.elns.error |
| `vali` | `integer` | `int` |  |  | Product Variant Valid. Allowed values: 1, 2 |
| `vali_kw` | `string` | `varchar` |  |  | Product Variant Valid (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcfd` | `timestamp` | `timestamp_ntz` |  |  | Configuration Date |
| `olid` | `string` | `varchar` |  |  | Configuration. Max length: 9 |
| `acfv` | `integer` | `int` |  |  | CPQ Configurator Variant. Allowed values: 1, 2 |
| `acfv_kw` | `string` | `varchar` |  |  | CPQ Configurator Variant (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acfs` | `integer` | `int` |  |  | CPQ Status. Allowed values: 1, 2, 3, 4, 5 |
| `acfs_kw` | `string` | `varchar` |  |  | CPQ Status (keyword). Allowed values: tipcf.acfs.not.applicable, tipcf.acfs.initial, tipcf.acfs.success, tipcf.acfs.partial.save, tipcf.acfs.error |
| `spcf` | `integer` | `int` |  |  | Structure by PCF. Allowed values: 1, 2 |
| `spcf_kw` | `string` | `varchar` |  |  | Structure by PCF (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `irft` | `integer` | `int` |  |  | Initial Reference Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `irft_kw` | `string` | `varchar` |  |  | Initial Reference Type (keyword). Allowed values: tcreft.sls.quotation, tcreft.sls.order, tcreft.budget, tcreft.project, tcreft.standard, tcreft.tp, tcreft.psd.ord, tcreft.contract, tcreft.sls.rco |
| `irfo` | `string` | `varchar` |  |  | Initial Reference Order. Max length: 9 |
| `enho` | `integer` | `int` |  |  | Engineering Hold. Allowed values: 1, 2 |
| `enho_kw` | `string` | `varchar` |  |  | Engineering Hold (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qana` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `site` | `string` | `varchar` |  |  | Sales Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `ccty` | `string` | `varchar` |  |  | Ship to Country. Max length: 3 |
| `prds` | `string` | `varchar` |  |  | Production Site. Max length: 9 |
| `alws__en_us` | `string` | `varchar` |  | `not_null` | (required) Alternative Workspace - base language. Max length: 50 |
| `txta` | `integer` | `int` |  |  | Product Variant Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tiipd001 Items - Production |
| `cuno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
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
