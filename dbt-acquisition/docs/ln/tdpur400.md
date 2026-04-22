# tdpur400

LN tdpur400 Purchase Orders table, Generated 2026-04-10T19:41:21Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur400` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno` |
| **Column count** | 198 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `otad` | `string` | `varchar` |  |  | Buy-from Address. Max length: 9 |
| `otcn` | `string` | `varchar` |  |  | Buy-from Contact. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `ifad` | `string` | `varchar` |  |  | Invoice-from Address. Max length: 9 |
| `ifcn` | `string` | `varchar` |  |  | Invoice-from Contact. Max length: 9 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `ptad` | `string` | `varchar` |  |  | Pay-to Address. Max length: 9 |
| `ptcn` | `string` | `varchar` |  |  | Pay-to Contact. Max length: 9 |
| `corg` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 29, 30, 99 |
| `corg_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sls.schedule, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.subc.pur.sched, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit, tdpur.corg.price.calc, tdpur.corg.not.appl |
| `cotp` | `string` | `varchar` |  |  | Purchase Order Type. Max length: 3 |
| `ragr` | `string` | `varchar` |  |  | Remittance Agreement. Max length: 16 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `odis` | `float` | `float` |  |  | Order Discount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `mcfr` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `mcfr_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ratp_1` | `float` | `float` |  |  | Purchase Rate |
| `ratp_2` | `float` | `float` |  |  | Purchase Rate |
| `ratp_3` | `float` | `float` |  |  | Purchase Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratt` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `raur` | `integer` | `int` |  |  | Use Purchase Rates for Receipts. Allowed values: 1, 2 |
| `raur_kw` | `string` | `varchar` |  |  | Use Purchase Rates for Receipts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `plnr` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cplp` | `string` | `varchar` |  |  | Purchase Price List. Max length: 3 |
| `bppr` | `string` | `varchar` |  |  | BP Prices/Discounts. Max length: 9 |
| `bptx` | `string` | `varchar` |  |  | BP Texts. Max length: 9 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Receipt Date |
| `ddtc` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Receipt Date |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `prno` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `ctrj` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `fdpt` | `string` | `varchar` |  |  | Financial Department. Max length: 6 |
| `odty` | `integer` | `int` |  |  | Original Document Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `odty_kw` | `string` | `varchar` |  |  | Original Document Type (keyword). Allowed values: tdpur.odty.order, tdpur.odty.schedule, tdpur.odty.receipt, tdpur.odty.sched.receipt, tdpur.odty.receipt.ident, tdpur.odty.notappl |
| `odno` | `string` | `varchar` |  |  | Original Order. Max length: 9 |
| `retr` | `string` | `varchar` |  |  | Return Reason. Max length: 6 |
| `sorn__en_us` | `string` | `varchar` |  | `not_null` | (required) Buy-from Business Partner Order - base language. Max length: 30 |
| `cosn` | `string` | `varchar` |  |  | Change Order Sequence No.. Max length: 8 |
| `akcd` | `string` | `varchar` |  |  | Acknowledgment. Max length: 2 |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `egen` | `integer` | `int` |  |  | EDI Message Generated. Allowed values: 1, 2 |
| `egen_kw` | `string` | `varchar` |  |  | EDI Message Generated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paft` | `integer` | `int` |  |  | Invoice after. Allowed values: 1, 2 |
| `paft_kw` | `string` | `varchar` |  |  | Invoice after (keyword). Allowed values: tcpaft.approval, tcpaft.receipts |
| `sbmt` | `string` | `varchar` |  |  | Self-Billing Method. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `oamt` | `float` | `float` |  |  | Order Amount |
| `comm` | `integer` | `int` |  |  | For Commingling. Allowed values: 1, 2 |
| `comm_kw` | `string` | `varchar` |  |  | For Commingling (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iebp` | `integer` | `int` |  |  | Invoice External Business Partner. Allowed values: 1, 2 |
| `iebp_kw` | `string` | `varchar` |  |  | Invoice External Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iafc` | `integer` | `int` |  |  | Invoice Freight Costs Based On. Allowed values: 5, 10, 15, 20 |
| `iafc_kw` | `string` | `varchar` |  |  | Invoice Freight Costs Based On (keyword). Allowed values: tccom.infr.estimate, tccom.infr.actual, tccom.infr.costplus, tccom.infr.not.applic |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `hdst` | `integer` | `int` |  |  | Status. Allowed values: 5, 10, 15, 20, 25, 28, 30, 35, 40, 45, 50 |
| `hdst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdpur.hdst.created, tdpur.hdst.approved, tdpur.hdst.sent, tdpur.hdst.in.process, tdpur.hdst.closed, tdpur.hdst.canc.in.proc, tdpur.hdst.cancelled, tdpur.hdst.modified, tdpur.hdst.blocked, tdpur.hdst.released, tdpur.hdst.not.applicable |
| `hisp` | `integer` | `int` |  |  | Log Order History. Allowed values: 1, 2 |
| `hisp_kw` | `string` | `varchar` |  |  | Log Order History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hism` | `integer` | `int` |  |  | Start Logging Order History at. Allowed values: 10, 20 |
| `hism_kw` | `string` | `varchar` |  |  | Start Logging Order History at (keyword). Allowed values: tdgen.hism.entry, tdgen.hism.approval |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `chrq` | `integer` | `int` |  |  | Change Request. Allowed values: 1, 2, 3 |
| `chrq_kw` | `string` | `varchar` |  |  | Change Request (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `revn` | `integer` | `int` |  |  | Revision |
| `opor` | `string` | `varchar` |  |  | Original Purchase Order. Max length: 9 |
| `crby` | `string` | `varchar` |  |  | Change Request Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Change Request Creation Date |
| `cror` | `integer` | `int` |  |  | Change Request Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 29, 30, 99 |
| `cror_kw` | `string` | `varchar` |  |  | Change Request Origin (keyword). Allowed values: tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sls.schedule, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.subc.pur.sched, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit, tdpur.corg.price.calc, tdpur.corg.not.appl |
| `crcl` | `integer` | `int` |  |  | Change Request Canceled. Allowed values: 1, 2 |
| `crcl_kw` | `string` | `varchar` |  |  | Change Request Canceled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crin` | `integer` | `int` |  |  | Internal Change Request. Allowed values: 1, 2 |
| `crin_kw` | `string` | `varchar` |  |  | Internal Change Request (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crrq` | `integer` | `int` |  |  | Change Request Required. Allowed values: 1, 2 |
| `crrq_kw` | `string` | `varchar` |  |  | Change Request Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcrq` | `integer` | `int` |  |  | Letter of Credit Required. Allowed values: 1, 2 |
| `lcrq_kw` | `string` | `varchar` |  |  | Letter of Credit Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payments. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spss` | `string` | `varchar` |  |  | Stage Payment Schedule Set. Max length: 3 |
| `sclb` | `integer` | `int` |  |  | Synced to Collaboration. Allowed values: 5, 10, 15, 99 |
| `sclb_kw` | `string` | `varchar` |  |  | Synced to Collaboration (keyword). Allowed values: tcgen.sclb.yes, tcgen.sclb.no, tcgen.sclb.partially.synce, tcgen.sclb.not.appl |
| `rsta` | `integer` | `int` |  |  | Response Status. Allowed values: 5, 10, 20, 30, 40, 50, 255 |
| `rsta_kw` | `string` | `varchar` |  |  | Response Status (keyword). Allowed values: tcspd.rsta.applicable, tcspd.rsta.pend.acceptance, tcspd.rsta.accepted, tcspd.rsta.declined, tcspd.rsta.partially.accep, tcspd.rsta.partially.decli, tcspd.rsta.not.appl |
| `chor` | `integer` | `int` |  |  | Change Origin. Allowed values: 10, 20, 30, 40, 90 |
| `chor_kw` | `string` | `varchar` |  |  | Change Origin (keyword). Allowed values: tdgen.chor.contr, tdgen.chor.subcontr, tdgen.chor.buyer, tdgen.chor.supplier, tdgen.chor.not.appl |
| `accr` | `integer` | `int` |  |  | Acceptance Required by Supplier. Allowed values: 1, 2 |
| `accr_kw` | `string` | `varchar` |  |  | Acceptance Required by Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngot` | `integer` | `int` |  |  | Negotiations Allowed by Supplier. Allowed values: 1, 2 |
| `ngot_kw` | `string` | `varchar` |  |  | Negotiations Allowed by Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `crht` | `integer` | `int` |  |  | Change Request Text |
| `cdf_adat` | `timestamp` | `timestamp_ntz` |  |  | Approved Date |
| `cdf_ausr` | `string` | `varchar` |  |  | Approved By. Max length: 16 |
| `cdf_cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cdf_crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `cdf_lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `cdf_lmus` | `string` | `varchar` |  |  | Last Modified By. Max length: 16 |
| `cdf_poid` | `string` | `varchar` |  |  | EAM PO. Max length: 10 |
| `cdf_usid` | `string` | `varchar` |  |  | Created By. Max length: 16 |
| `orno_cosn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur414 Purchase Change Order Sequence Numbers |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `otad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `otcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ifad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ifcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ptad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ptcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cotp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ratt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `plnr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cplp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `bptx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `prno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur300 Purchase Contracts |
| `ctrj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `fdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `retr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `akcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur054 Purchase Acknowledgments |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `sbmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs057 Self-Billing Methods |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `spss_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs240 Installment Schedules Sets |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `crht_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ratc_lclc` | `float` | `float` |  |  | CC: Transaction Currency to Local Currency |
| `ratc_rpc1` | `float` | `float` |  |  | CC: Transaction Currency to Reporting Currency 1 |
| `ratc_rpc2` | `float` | `float` |  |  | CC: Transaction Currency to Reporting Currency 2 |
| `ratc_rfrc` | `float` | `float` |  |  | CC: Transaction Currency to Reference Currency |
| `ratc_dtwc` | `float` | `float` |  |  | CC: Transaction Currency to Data Warehouse Currency |
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
