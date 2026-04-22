# tdipu001

LN tdipu001 Item - Purchase table, Generated 2026-04-10T19:41:16Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdipu001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item` |
| **Column count** | 106 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key I - base language. Max length: 16 |
| `cuqi` | `string` | `varchar` |  |  | Purchase Piece Unit. Max length: 3 |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `rqun` | `string` | `varchar` |  |  | Rental Purchase Unit. Max length: 3 |
| `cupp` | `string` | `varchar` |  |  | Purchase Price Unit. Max length: 3 |
| `rpun` | `string` | `varchar` |  |  | Rental Purchase Price Unit. Max length: 3 |
| `cpgp` | `string` | `varchar` |  |  | Purchase Price Group. Max length: 6 |
| `csgp` | `string` | `varchar` |  |  | Purchase Statistics Group. Max length: 6 |
| `ccur` | `string` | `varchar` |  |  | Purchase Currency. Max length: 3 |
| `vtbo` | `integer` | `int` |  |  | VAT Based on. Allowed values: 10, 20, 100 |
| `vtbo_kw` | `string` | `varchar` |  |  | VAT Based on (keyword). Allowed values: tctax.vtbo.services, tctax.vtbo.goods, tctax.vtbo.not.appl |
| `prip` | `float` | `float` |  |  | Purchase Price |
| `rppr` | `float` | `float` |  |  | Rental Purchase Price |
| `ispr` | `float` | `float` |  |  | Product Subcontracting Purchase Price |
| `scpr` | `float` | `float` |  |  | Operation Subcontracting Purchase Price |
| `sopr` | `integer` | `int` |  |  | Source of Price for Operation Subcontracting. Allowed values: 1, 2, 3, 4 |
| `sopr_kw` | `string` | `varchar` |  |  | Source of Price for Operation Subcontracting (keyword). Allowed values: tcsopr.rate, tcsopr.price.book, tcsopr.not.appl, tcsopr.reference.act |
| `sspr` | `float` | `float` |  |  | Service Subcontracting Purchase Price |
| `sops` | `integer` | `int` |  |  | Source of Price for Service Subcontracting. Allowed values: 1, 2, 3, 4 |
| `sops_kw` | `string` | `varchar` |  |  | Source of Price for Service Subcontracting (keyword). Allowed values: tcsopr.rate, tcsopr.price.book, tcsopr.not.appl, tcsopr.reference.act |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `buyr` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `edco` | `integer` | `int` |  |  | Effective Date by Change Order. Allowed values: 1, 2 |
| `edco_kw` | `string` | `varchar` |  |  | Effective Date by Change Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mlco` | `integer` | `int` |  |  | Multiple Change Orders. Allowed values: 1, 2 |
| `mlco_kw` | `string` | `varchar` |  |  | Multiple Change Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rtdp` | `integer` | `int` |  |  | Delivery Date Tolerance (+) |
| `rtdm` | `integer` | `int` |  |  | Delivery Date Tolerance (-) |
| `rtqp` | `float` | `float` |  |  | Quantity Tolerance (+) |
| `rtqm` | `float` | `float` |  |  | Quantity Tolerance (-) |
| `acci` | `integer` | `int` |  |  | Accessories Allowed. Allowed values: 1, 2 |
| `acci_kw` | `string` | `varchar` |  |  | Accessories Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mmnf` | `integer` | `int` |  |  | Multiple Manufacturer Item. Allowed values: 1, 2 |
| `mmnf_kw` | `string` | `varchar` |  |  | Multiple Manufacturer Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cims` | `integer` | `int` |  |  | Check for Valid Item-Manufacturer / Item-Manufacturer and BP. Allowed values: 1, 2 |
| `cims_kw` | `string` | `varchar` |  |  | Check for Valid Item-Manufacturer / Item-Manufacturer and BP (keyword). Allowed values: tdipu.cims.item.cmnf, tdipu.cims.item.cmnf.otbp |
| `lcmp` | `integer` | `int` |  |  | Purchase Office Logistic Company |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `retw` | `integer` | `int` |  |  | Release to Warehousing. Allowed values: 1, 2 |
| `retw_kw` | `string` | `varchar` |  |  | Release to Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payments. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `afrb` | `integer` | `int` |  |  | Item applicable for Retro-Billing. Allowed values: 1, 2 |
| `afrb_kw` | `string` | `varchar` |  |  | Item applicable for Retro-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spco` | `integer` | `int` |  |  | Specify Cost Optionally. Allowed values: 1, 2 |
| `spco_kw` | `string` | `varchar` |  |  | Specify Cost Optionally (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vryn` | `integer` | `int` |  |  | Vendor Rating. Allowed values: 1, 2 |
| `vryn_kw` | `string` | `varchar` |  |  | Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `suti` | `float` | `float` |  |  | Supply Time |
| `sutu` | `integer` | `int` |  |  | Unit for Supply Time. Allowed values: 5, 10 |
| `sutu_kw` | `string` | `varchar` |  |  | Unit for Supply Time (keyword). Allowed values: tctope.hours, tctope.days |
| `qual` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `qual_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `hstq` | `integer` | `int` |  |  | Hard Stop on Quantity. Allowed values: 1, 2, 3 |
| `hstq_kw` | `string` | `varchar` |  |  | Hard Stop on Quantity (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `hstd` | `integer` | `int` |  |  | Hard Stop on Date. Allowed values: 1, 2, 3 |
| `hstd_kw` | `string` | `varchar` |  |  | Hard Stop on Date (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `casl` | `integer` | `int` |  |  | Deliver by Specified Suppliers Only. Allowed values: 1, 2 |
| `casl_kw` | `string` | `varchar` |  |  | Deliver by Specified Suppliers Only (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mpyn` | `integer` | `int` |  |  | MPN Item. Allowed values: 1, 2 |
| `mpyn_kw` | `string` | `varchar` |  |  | MPN Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mpnr` | `string` | `varchar` |  |  | Preferred Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `rqms` | `integer` | `int` |  |  | Requisition Mandatory for Operation Subcontracting. Allowed values: 1, 2 |
| `rqms_kw` | `string` | `varchar` |  |  | Requisition Mandatory for Operation Subcontracting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rmss` | `integer` | `int` |  |  | Requisition Mandatory for Service Subcontracting. Allowed values: 1, 2 |
| `rmss_kw` | `string` | `varchar` |  |  | Requisition Mandatory for Service Subcontracting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scus` | `integer` | `int` |  |  | Purchase Schedule in use. Allowed values: 1, 2 |
| `scus_kw` | `string` | `varchar` |  |  | Purchase Schedule in use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `styp` | `integer` | `int` |  |  | Schedule Type. Allowed values: 1, 2, 3 |
| `styp_kw` | `string` | `varchar` |  |  | Schedule Type (keyword). Allowed values: tcstyp.not.applicable, tcstyp.pull, tcstyp.push |
| `ctyo` | `string` | `varchar` |  |  | Country of Origin. Max length: 3 |
| `txtp` | `integer` | `int` |  |  | Item Purchase Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cuqi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rqun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rpun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cpgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `csgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `buyr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `ctyo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `txtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
