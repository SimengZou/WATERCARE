# tdsls400

LN tdsls400 Sales Orders table, Generated 2026-04-10T19:41:25Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls400` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno` |
| **Column count** | 200 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Sales Order. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `corg` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 21, 22, 25, 30, 35, 40, 45, 99 |
| `corg_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdsls.corg.contracts, tdsls.corg.quotations, tdsls.corg.eop, tdsls.corg.manual, tdsls.corg.phone, tdsls.corg.fax, tdsls.corg.mail, tdsls.corg.opportunity, tdsls.corg.crm, tdsls.corg.consumption, tdsls.corg.quicklist, tdsls.corg.service, tdsls.corg.edi.dir.del, tdsls.corg.retrobill, tdsls.corg.planning, tdsls.corg.purchase, tdsls.corg.shipment, tdsls.corg.price.calc, tdsls.corg.not.appl |
| `sotp` | `string` | `varchar` |  |  | Sales Order Type. Max length: 3 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `mprm` | `integer` | `int` |  |  | Multiple Promotion Method. Allowed values: 1, 2 |
| `mprm_kw` | `string` | `varchar` |  |  | Multiple Promotion Method (keyword). Allowed values: tdsls.mprm.parallel, tdsls.mprm.cumulative |
| `tprd` | `float` | `float` |  |  | Total Promotion Discount |
| `odis` | `float` | `float` |  |  | Order Discount |
| `ccur` | `string` | `varchar` |  |  | Order Currency. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `rats_1` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_2` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_3` | `float` | `float` |  |  | Currency Rate Sales |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratt` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cpls` | `string` | `varchar` |  |  | Sales Price List. Max length: 3 |
| `pldd` | `string` | `varchar` |  |  | Price List for Direct Delivery. Max length: 3 |
| `bppr` | `string` | `varchar` |  |  | BP Prices/Discounts. Max length: 9 |
| `bptx` | `string` | `varchar` |  |  | Business Partner Texts. Max length: 9 |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `pcod` | `string` | `varchar` |  |  | Delivery Pattern. Max length: 6 |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `ddtc` | `timestamp` | `timestamp_ntz` |  |  | Customer Requested Delivery Date |
| `futo` | `integer` | `int` |  |  | Invoicing by Installments. Allowed values: 1, 2, 3, 4, 10 |
| `futo_kw` | `string` | `varchar` |  |  | Invoicing by Installments (keyword). Allowed values: tcfuto.no, tcfuto.direct, tcfuto.indirect, tcfuto.delivered, tcfuto.plan |
| `inpl` | `string` | `varchar` |  |  | Installment Plan. Max length: 9 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 30 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `prno` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tdscon.none, tdscon.sc, tdscon.rc, tdscon.oc, tdscon.sca, tdscon.kc, tdscon.not.applicable |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `cfdt` | `timestamp` | `timestamp_ntz` |  |  | Order Confirmation Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Goods Acceptance Date |
| `odty` | `integer` | `int` |  |  | Original Document Type. Allowed values: 1, 2, 4, 5, 6, 7, 8, 9, 15 |
| `odty_kw` | `string` | `varchar` |  |  | Original Document Type (keyword). Allowed values: tdsls.odty.order, tdsls.odty.shipment, tdsls.odty.invoice, tdsls.odty.schedule, tdsls.odty.sch.shipment, tdsls.odty.rb.order, tdsls.odty.rb.schedule, tdsls.odty.shipment.ident, tdsls.odty.notappl |
| `odno` | `string` | `varchar` |  |  | Original Document Number. Max length: 9 |
| `rcmp` | `integer` | `int` |  |  | Return Invoice Company |
| `rtyp` | `string` | `varchar` |  |  | Return Transaction Type. Max length: 3 |
| `retr` | `string` | `varchar` |  |  | Return Reason. Max length: 6 |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `fdpt` | `string` | `varchar` |  |  | Financial Department. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `clyn` | `integer` | `int` |  |  | Canceled. Allowed values: 1, 2 |
| `clyn_kw` | `string` | `varchar` |  |  | Canceled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `bkyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `bkyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `agen` | `integer` | `int` |  |  | EDI Acknowledgement Generated. Allowed values: 1, 2 |
| `agen_kw` | `string` | `varchar` |  |  | EDI Acknowledgement Generated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cosn` | `string` | `varchar` |  |  | Change Order Sequence No.. Max length: 8 |
| `akcd` | `string` | `varchar` |  |  | Sales Acknowledgment. Max length: 2 |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oref__en_us` | `string` | `varchar` |  | `not_null` | (required) Order Reference - base language. Max length: 30 |
| `frin` | `integer` | `int` |  |  | Invoice for Freight. Allowed values: 1, 2 |
| `frin_kw` | `string` | `varchar` |  |  | Invoice for Freight (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `infr` | `integer` | `int` |  |  | Invoice Freight Costs Based On. Allowed values: 5, 10, 15, 20 |
| `infr_kw` | `string` | `varchar` |  |  | Invoice Freight Costs Based On (keyword). Allowed values: tccom.infr.estimate, tccom.infr.actual, tccom.infr.costplus, tccom.infr.not.applic |
| `oamt` | `float` | `float` |  |  | Order Amount |
| `ruso` | `integer` | `int` |  |  | Rush Order. Allowed values: 1, 2 |
| `ruso_kw` | `string` | `varchar` |  |  | Rush Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hdst` | `integer` | `int` |  |  | Sales Order Status. Allowed values: 2, 5, 10, 20, 25, 30, 35, 40, 45, 50 |
| `hdst_kw` | `string` | `varchar` |  |  | Sales Order Status (keyword). Allowed values: tdsls.hdst.suspended, tdsls.hdst.free, tdsls.hdst.approved, tdsls.hdst.in.process, tdsls.hdst.modified, tdsls.hdst.closed, tdsls.hdst.cancelled, tdsls.hdst.blocked, tdsls.hdst.released, tdsls.hdst.not.applicable |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `shpm` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `hiss` | `integer` | `int` |  |  | Log Order History. Allowed values: 1, 2 |
| `hiss_kw` | `string` | `varchar` |  |  | Log Order History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ehis` | `integer` | `int` |  |  | Log EDI Order History. Allowed values: 1, 2 |
| `ehis_kw` | `string` | `varchar` |  |  | Log EDI Order History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hism` | `integer` | `int` |  |  | Start Logging Order History at. Allowed values: 10, 20 |
| `hism_kw` | `string` | `varchar` |  |  | Start Logging Order History at (keyword). Allowed values: tdgen.hism.entry, tdgen.hism.approval |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `esin` | `integer` | `int` |  |  | Extended Service Integration. Allowed values: 1, 2 |
| `esin_kw` | `string` | `varchar` |  |  | Extended Service Integration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcrq` | `integer` | `int` |  |  | Letter of Credit Required. Allowed values: 1, 2 |
| `lcrq_kw` | `string` | `varchar` |  |  | Letter of Credit Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `airp` | `integer` | `int` |  |  | Allow Inventory Recheck for Promised Lines. Allowed values: 1, 2 |
| `airp_kw` | `string` | `varchar` |  |  | Allow Inventory Recheck for Promised Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `chrq` | `integer` | `int` |  |  | Change Request. Allowed values: 1, 2, 3 |
| `chrq_kw` | `string` | `varchar` |  |  | Change Request (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `revn` | `integer` | `int` |  |  | Change Request Revision |
| `osor` | `string` | `varchar` |  |  | Original Sales Order. Max length: 9 |
| `crby` | `string` | `varchar` |  |  | Change Request Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Change Request Creation Date |
| `cror` | `integer` | `int` |  |  | Change Request Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 21, 22, 25, 30, 35, 40, 45, 99 |
| `cror_kw` | `string` | `varchar` |  |  | Change Request Origin (keyword). Allowed values: tdsls.corg.contracts, tdsls.corg.quotations, tdsls.corg.eop, tdsls.corg.manual, tdsls.corg.phone, tdsls.corg.fax, tdsls.corg.mail, tdsls.corg.opportunity, tdsls.corg.crm, tdsls.corg.consumption, tdsls.corg.quicklist, tdsls.corg.service, tdsls.corg.edi.dir.del, tdsls.corg.retrobill, tdsls.corg.planning, tdsls.corg.purchase, tdsls.corg.shipment, tdsls.corg.price.calc, tdsls.corg.not.appl |
| `crin` | `integer` | `int` |  |  | Internal Change Request. Allowed values: 1, 2 |
| `crin_kw` | `string` | `varchar` |  |  | Internal Change Request (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crst` | `integer` | `int` |  |  | Change Request Status. Allowed values: 10, 20, 30, 40, 50, 90 |
| `crst_kw` | `string` | `varchar` |  |  | Change Request Status (keyword). Allowed values: tdgen.crst.initial, tdgen.crst.approved, tdgen.crst.modified, tdgen.crst.processed, tdgen.crst.canceled, tdgen.crst.not.applicable |
| `crrq` | `integer` | `int` |  |  | Change Request Required. Allowed values: 1, 2 |
| `crrq_kw` | `string` | `varchar` |  |  | Change Request Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `crht` | `integer` | `int` |  |  | Change Request Text |
| `orno_cosn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls414 Sales Change Order Sequence Numbers |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sotp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls094 Sales Order Types |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ratt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cpls_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `pldd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `bptx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `pcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa021 Delivery Patterns |
| `inpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs245 Installment Plans |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `prno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `retr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `fdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `akcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls054 Sales Acknowledgments |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls098 Change Types |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `crht_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `tprd_lclc` | `float` | `float` |  |  | CC: Order Promotion Discount in Local Currency |
| `tprd_rpc1` | `float` | `float` |  |  | CC: Order Promotion Discount in Reporting Currency 1 |
| `tprd_rpc2` | `float` | `float` |  |  | CC: Order Promotion Discount in Reporting Currency 2 |
| `tprd_rfrc` | `float` | `float` |  |  | CC: Order Promotion Discount in Reference Currency |
| `tprd_dtwc` | `float` | `float` |  |  | CC: Order Promotion Discount in Data Warehouse Currency |
| `cofc_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Sales Office |
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
