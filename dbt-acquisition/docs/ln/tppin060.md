# tppin060

LN tppin060 Project Shipments table, Generated 2026-04-10T19:42:05Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin060` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `dlvr`, `schd`, `cprj`, `cspa`, `cpla`, `cact`, `shpm`, `pono`, `rcno`, `rcln`, `sern` |
| **Column count** | 106 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `dlvr` | `integer` | `int` | `PK` | `not_null` | (required) Deliverable |
| `schd` | `integer` | `int` | `PK` | `not_null` | (required) Schedule |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Shipment. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Shipment Line |
| `rcno` | `string` | `varchar` | `PK` | `not_null` | (required) Receipt. Max length: 9 |
| `rcln` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Line |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Delivery Sequence |
| `dlor` | `integer` | `int` |  |  | Origin. Allowed values: 10, 20 |
| `dlor_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tpctm.dlor.contract, tpctm.dlor.project |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `lssn` | `string` | `varchar` |  |  | Item Identification Set. Max length: 22 |
| `qshp` | `float` | `float` |  |  | Delivered Quantity |
| `suni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `addt` | `timestamp` | `timestamp_ntz` |  |  | Actual Delivery Time |
| `dlld` | `timestamp` | `timestamp_ntz` |  |  | Delivery Log Date |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `sacu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `samt` | `float` | `float` |  |  | Sales Amount |
| `nins` | `integer` | `int` |  |  | Installment |
| `icmp` | `integer` | `int` |  |  | Invoice Company |
| `fcmp` | `integer` | `int` |  |  | Invoice Document Company |
| `ityp` | `string` | `varchar` |  |  | Invoice Document Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document Number |
| `idln` | `integer` | `int` |  |  | Invoice Document Line |
| `link` | `integer` | `int` |  |  | Linked to Invoice. Allowed values: 0, 1, 2 |
| `link_kw` | `string` | `varchar` |  |  | Linked to Invoice (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `twar` | `string` | `varchar` |  |  | To Warehouse. Max length: 6 |
| `citt` | `string` | `varchar` |  |  | Item Coding System. Max length: 3 |
| `srvc` | `integer` | `int` |  |  | Item is in Service. Allowed values: 1, 2 |
| `srvc_kw` | `string` | `varchar` |  |  | Item is in Service (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `rtor` | `integer` | `int` |  |  | Return Deliverable. Allowed values: 1, 2 |
| `rtor_kw` | `string` | `varchar` |  |  | Return Deliverable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `dlnt` | `string` | `varchar` |  |  | Delivery Note. Max length: 19 |
| `wght` | `float` | `float` |  |  | Gross Weight |
| `cwun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `puni` | `string` | `varchar` |  |  | Price Unit. Max length: 3 |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20, 30, 50, 200, 205, 210, 215 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tpgen.porg.not.applicable, tpgen.porg.manual, tpgen.porg.contract, tpgen.porg.variant, tpgen.porg.item.pur, tpgen.porg.item.sls, tpgen.porg.item.service, tpgen.porg.supp.pr.book, tpgen.porg.def.pr.book, tpgen.porg.price.structure, tpgen.porg.extern, tpgen.porg.generic.pr.list, tpgen.porg.budget, tpgen.porg.equipment.proj, tpgen.porg.equipment.stand, tpgen.porg.subcontr.proj, tpgen.porg.subcontr.stand |
| `ldam` | `float` | `float` |  |  | Discount Amount |
| `disc` | `float` | `float` |  |  | Discount Percentage |
| `dorg` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12, 200, 205, 210, 215 |
| `dorg_kw` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tpgen.dorg.not.applicable, tpgen.dorg.manual, tpgen.dorg.contract, tpgen.dorg.disc.structure, tpgen.dorg.disc.pr.book, tpgen.dorg.extern, tpgen.dorg.equipment.proj, tpgen.dorg.equipment.stand, tpgen.dorg.subcontr.proj, tpgen.dorg.subcontr.stand |
| `pcad` | `integer` | `int` |  |  | Price Changes Allowed. Allowed values: 1, 2 |
| `pcad_kw` | `string` | `varchar` |  |  | Price Changes Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pupd` | `integer` | `int` |  |  | Price Updated. Allowed values: 1, 2 |
| `pupd_kw` | `string` | `varchar` |  |  | Price Updated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `lssn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd410 Item Identification Sets |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `twar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `samt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `samt_cntc` | `float` | `float` |  |  | CC: Sales Amount in Contract Currency |
| `samt_prjc` | `float` | `float` |  |  | CC: Sales Amount in Project Currency |
| `samt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `samt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `samt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `samt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
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
