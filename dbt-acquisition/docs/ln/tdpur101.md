# tdpur101

LN tdpur101 Request for Quotation Lines table, Generated 2026-04-10T19:41:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur101` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `qono`, `pono`, `srnb` |
| **Column count** | 105 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `qono` | `string` | `varchar` | `PK` | `not_null` | (required) RFQ. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `srnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `ltyp` | `integer` | `int` |  |  | Line Type. Allowed values: 5, 10, 15 |
| `ltyp_kw` | `string` | `varchar` |  |  | Line Type (keyword). Allowed values: tcgen.ltyp.total, tcgen.ltyp.detail, tcgen.ltyp.line |
| `kofl` | `integer` | `int` |  |  | Kind of Line. Allowed values: 5, 10, 15 |
| `kofl_kw` | `string` | `varchar` |  |  | Kind of Line (keyword). Allowed values: tcgen.kofl.primary, tcgen.kofl.alternative, tcgen.kofl.not.appl |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `qspa` | `integer` | `int` |  |  | Status. Allowed values: 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60 |
| `qspa_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdpur.rfqs.created, tdpur.rfqs.pending.resp, tdpur.rfqs.sent, tdpur.rfqs.modified, tdpur.rfqs.responded, tdpur.rfqs.in.process, tdpur.rfqs.no.bid, tdpur.rfqs.no.response, tdpur.rfqs.negotiating, tdpur.rfqs.accepted, tdpur.rfqs.rejected, tdpur.rfqs.processed, tdpur.rfqs.cancelled |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `citt` | `string` | `varchar` |  |  | Code Item Type. Max length: 3 |
| `crit__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `acqm` | `integer` | `int` |  |  | Acquisition Method. Allowed values: 5, 10, 99 |
| `acqm_kw` | `string` | `varchar` |  |  | Acquisition Method (keyword). Allowed values: tcacqm.buying, tcacqm.rental, tcacqm.not.appl |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `remn` | `string` | `varchar` |  |  | Requester. Max length: 9 |
| `qdat` | `timestamp` | `timestamp_ntz` |  |  | RFQ Date |
| `qoor` | `float` | `float` |  |  | RFQ Quantity |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `cvqp` | `float` | `float` |  |  | Purchase Unit Conversion Factor |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `rcsi` | `string` | `varchar` |  |  | Receiving Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `pres` | `float` | `float` |  |  | Target Price |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Response Date |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Receipt Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Start Date for Receipt Period |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | End Date for Receipt Period |
| `lead` | `integer` | `int` |  |  | Lead Time |
| `leun` | `integer` | `int` |  |  | Lead Time Unit. Allowed values: 5, 10 |
| `leun_kw` | `string` | `varchar` |  |  | Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `pegd` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `pegd_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scdt` | `timestamp` | `timestamp_ntz` |  |  | Score Date |
| `scac` | `integer` | `int` |  |  | Score Actual. Allowed values: 1, 2 |
| `scac_kw` | `string` | `varchar` |  |  | Score Actual (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `corg` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 29, 30, 99 |
| `corg_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sls.schedule, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.subc.pur.sched, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit, tdpur.corg.price.calc, tdpur.corg.not.appl |
| `ipla` | `integer` | `int` |  |  | Include in Planning. Allowed values: 1, 2 |
| `ipla_kw` | `string` | `varchar` |  |  | Include in Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `expo` | `integer` | `int` |  |  | RFQ Expected Outcome. Allowed values: 10, 20, 30, 40, 50, 100 |
| `expo_kw` | `string` | `varchar` |  |  | RFQ Expected Outcome (keyword). Allowed values: tdpur.cnvs.order, tdpur.cnvs.contract, tdpur.cnvs.pricebook, tdpur.cnvs.order.contract, tdpur.cnvs.orderpricebook, tdpur.cnvs.not.appl |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `mark` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `mark_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `org1` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `org2` | `integer` | `int` |  |  | Obsolete |
| `org3` | `integer` | `int` |  |  | Obsolete |
| `pseq` | `integer` | `int` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Line Text |
| `cdf_idet` | `string` | `varchar` |  |  | Item Detail. Max length: 100 |
| `qono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur100 Requests for Quotation |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `remn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rcsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
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
