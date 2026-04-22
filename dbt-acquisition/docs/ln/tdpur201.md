# tdpur201

LN tdpur201 Purchase Requisition Lines table, Generated 2026-04-10T19:41:19Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur201` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rqno`, `pono` |
| **Column count** | 79 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rqno` | `string` | `varchar` | `PK` | `not_null` | (required) Requisition. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `nids__en_us` | `string` | `varchar` |  | `not_null` | (required) Item Description - base language. Max length: 80 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `citt` | `string` | `varchar` |  |  | Item Code System. Max length: 3 |
| `crit__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `mpnr` | `string` | `varchar` |  |  | Preferred Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mitm__en_us` | `string` | `varchar` |  | `not_null` | (required) Manufacturer Item - base language. Max length: 35 |
| `acqm` | `integer` | `int` |  |  | Acquisition Method. Allowed values: 5, 10, 99 |
| `acqm_kw` | `string` | `varchar` |  |  | Acquisition Method (keyword). Allowed values: tcacqm.buying, tcacqm.rental, tcacqm.not.appl |
| `qoor` | `float` | `float` |  |  | Order Quantity |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `cvqp` | `float` | `float` |  |  | Purchase Unit Conversion Factor |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `nsds__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Partner Description - base language. Max length: 50 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Requested Date |
| `pric` | `float` | `float` |  |  | Price |
| `cupp` | `string` | `varchar` |  |  | Price Unit. Max length: 3 |
| `cvpp` | `float` | `float` |  |  | Price Unit Conversion Factor |
| `oamt` | `float` | `float` |  |  | Order Amount |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `glco` | `string` | `varchar` |  |  | General Ledger. Max length: 135 |
| `rejc` | `integer` | `int` |  |  | Rejected. Allowed values: 1, 2 |
| `rejc_kw` | `string` | `varchar` |  |  | Rejected (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcod` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `urgt` | `integer` | `int` |  |  | Urgent. Allowed values: 1, 2 |
| `urgt_kw` | `string` | `varchar` |  |  | Urgent (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnty` | `integer` | `int` |  |  | Conversion Type. Allowed values: 1, 2, 3 |
| `cnty_kw` | `string` | `varchar` |  |  | Conversion Type (keyword). Allowed values: tdpur.cnty.none, tdpur.cnty.rfq, tdpur.cnty.pur |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pegd` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `pegd_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `cpla` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Line Text |
| `cdf_idet` | `string` | `varchar` |  |  | Item Detail. Max length: 100 |
| `rqno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur200 Purchase Requisitions |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
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
