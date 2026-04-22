# tdpur411

LN tdpur411 Purchase Order Line Item Data table, Generated 2026-04-10T19:41:22Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur411` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb` |
| **Column count** | 42 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `atsg` | `string` | `varchar` |  |  | Attribute set group code. Max length: 20 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `cpcl` | `string` | `varchar` |  |  | Product Class. Max length: 6 |
| `cpln` | `string` | `varchar` |  |  | Product Line. Max length: 6 |
| `prtp` | `string` | `varchar` |  |  | Product Type. Max length: 3 |
| `cpgp` | `string` | `varchar` |  |  | Purchase Price Group. Max length: 6 |
| `csgp` | `string` | `varchar` |  |  | Purchase Statistics Group. Max length: 6 |
| `iwgt` | `float` | `float` |  |  | Item Weight |
| `iwun` | `string` | `varchar` |  |  | Item Weight Unit. Max length: 3 |
| `pgmd` | `integer` | `int` |  |  | Peg Mandatory. Allowed values: 1, 2 |
| `pgmd_kw` | `string` | `varchar` |  |  | Peg Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `kitm` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 35, 40, 50, 60, 70, 80, 90, 100, 110 |
| `kitm_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tckitm.purchase, tckitm.manufacture, tckitm.generic.obs, tckitm.cost.obs, tckitm.service.obs, tckitm.subcontract.obs, tckitm.list.obs, tckitm.tool.obs, tckitm.equipment.obs, tckitm.engineering.obs, tckitm.product, tckitm.rental.prod, tckitm.tool, tckitm.equipment, tckitm.subcontracting, tckitm.cost, tckitm.service, tckitm.generic, tckitm.engineering, tckitm.list |
| `orno_pono_sqnb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur401 Purchase Order Lines |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur400 Purchase Orders |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd030 Attribute Sets |
| `atsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs026 Attribute Set Groups |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `cpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs062 Product Classes |
| `cpln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs061 Product Lines |
| `prtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs015 Product Types |
| `cpgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `csgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `iwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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
