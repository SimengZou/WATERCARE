# whinh430

LN whinh430 Shipments table, Generated 2026-04-10T19:42:49Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh430` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `shpm` |
| **Column count** | 206 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Shipment. Max length: 9 |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `seri` | `string` | `varchar` |  |  | Series. Max length: 8 |
| `sfcp` | `integer` | `int` |  |  | Ship-from Company |
| `sfty` | `integer` | `int` |  |  | Ship-from Type. Allowed values: 1, 2, 3, 4, 10 |
| `sfty_kw` | `string` | `varchar` |  |  | Ship-from Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `sfco` | `string` | `varchar` |  |  | Ship-from Code. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfsi` | `string` | `varchar` |  |  | Ship-from Site. Max length: 9 |
| `stcp` | `integer` | `int` |  |  | Ship-to Company |
| `stty` | `integer` | `int` |  |  | Ship-to Type. Allowed values: 1, 2, 3, 4, 10 |
| `stty_kw` | `string` | `varchar` |  |  | Ship-to Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `stco` | `string` | `varchar` |  |  | Ship-to Code. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stsi` | `string` | `varchar` |  |  | Ship-to Site. Max length: 9 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `sdtf` | `timestamp` | `timestamp_ntz` |  |  | Shipment Date From |
| `sdtt` | `timestamp` | `timestamp_ntz` |  |  | Shipment Date To |
| `fxwt` | `integer` | `int` |  |  | Fixed Weight. Allowed values: 0, 1, 2 |
| `fxwt_kw` | `string` | `varchar` |  |  | Fixed Weight (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `wght` | `float` | `float` |  |  | Gross Weight |
| `ntwt` | `float` | `float` |  |  | Net Weight |
| `cwun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `shst` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 10, 20, 30, 40 |
| `shst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.shst.open, whinh.shst.frozen, whinh.shst.confirmed, whinh.shst.part.frozen, whinh.shst.confirming, whinh.shst.projected, whinh.shst.canceled |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Date Confirmed |
| `koch` | `integer` | `int` |  |  | Kind of Charge. Allowed values: 1, 2, 3, 4, 10, 20, 30, 40, 150, 160, 200, 250 |
| `koch_kw` | `string` | `varchar` |  |  | Kind of Charge (keyword). Allowed values: whinh.koch.surcharge, whinh.koch.freight, whinh.koch.handling, whinh.koch.setup, whinh.koch.not.appl, whinh.koch.packing, whinh.koch.insurance, whinh.koch.commission, whinh.koch.miscellaneous, whinh.koch.other, whinh.koch.consolidated, whinh.koch.not.applicable |
| `curr` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cham` | `float` | `float` |  |  | Charge Amount |
| `stat` | `integer` | `int` |  |  | EDI Status. Allowed values: 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 20, 25, 30 |
| `stat_kw` | `string` | `varchar` |  |  | EDI Status (keyword). Allowed values: whinh.stat.prepare, whinh.stat.confirmed, whinh.stat.sent, whinh.stat.disapproved, whinh.stat.modify, whinh.stat.asn.received, whinh.stat.scheduled, whinh.stat.scheduled.man, whinh.stat.received, whinh.stat.under.review, whinh.stat.open, whinh.stat.cancelled, whinh.stat.replaced, whinh.stat.not.appl |
| `hazm` | `integer` | `int` |  |  | Hazardous Material. Allowed values: 1, 2 |
| `hazm_kw` | `string` | `varchar` |  |  | Hazardous Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `risk` | `string` | `varchar` |  |  | Class of Risk. Max length: 15 |
| `bolp` | `integer` | `int` |  |  | Bill of Lading Printed. Allowed values: 1, 2 |
| `bolp_kw` | `string` | `varchar` |  |  | Bill of Lading Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcsp` | `integer` | `int` |  |  | Packing Slip Printed. Allowed values: 1, 2 |
| `pcsp_kw` | `string` | `varchar` |  |  | Packing Slip Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pdat` | `timestamp` | `timestamp_ntz` |  |  | Packing Slip Date |
| `delp` | `integer` | `int` |  |  | Delivery Note Printed. Allowed values: 1, 2 |
| `delp_kw` | `string` | `varchar` |  |  | Delivery Note Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pclp` | `integer` | `int` |  |  | Packing List Printed. Allowed values: 1, 2 |
| `pclp_kw` | `string` | `varchar` |  |  | Packing List Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mapr` | `integer` | `int` |  |  | Shipping Manifest Printed. Allowed values: 1, 2 |
| `mapr_kw` | `string` | `varchar` |  |  | Shipping Manifest Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pfpr` | `integer` | `int` |  |  | Pro Forma Invoice Processed. Allowed values: 1, 2 |
| `pfpr_kw` | `string` | `varchar` |  |  | Pro Forma Invoice Processed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sflp` | `integer` | `int` |  |  | Shipment Planned for Load Plan. Allowed values: 1, 2 |
| `sflp_kw` | `string` | `varchar` |  |  | Shipment Planned for Load Plan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `depc` | `integer` | `int` |  |  | Office Company |
| `wdep` | `string` | `varchar` |  |  | Office. Max length: 6 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `taxs` | `string` | `varchar` |  |  | Own Identification Number. Max length: 35 |
| `taxp` | `string` | `varchar` |  |  | Business Partner Identification Number. Max length: 35 |
| `ttyp` | `string` | `varchar` |  |  | Invoice Transaction Type. Max length: 3 |
| `incp` | `integer` | `int` |  |  | Invoice Company |
| `invn` | `integer` | `int` |  |  | Invoice Number |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `minv` | `integer` | `int` |  |  | Multiple Invoices. Allowed values: 1, 2 |
| `minv_kw` | `string` | `varchar` |  |  | Multiple Invoices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fval` | `float` | `float` |  |  | Freight Value |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `ispr` | `string` | `varchar` |  |  | Shipment Procedure. Max length: 3 |
| `shrf` | `string` | `varchar` |  |  | Shipping Sequence Shipment Reference. Max length: 9 |
| `llsq` | `integer` | `int` |  |  | Loading List Sequence |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `prdn` | `string` | `varchar` |  |  | Preliminary Delivery Note. Max length: 9 |
| `manl` | `integer` | `int` |  |  | Manual. Allowed values: 1, 2 |
| `manl_kw` | `string` | `varchar` |  |  | Manual (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpro__en_us` | `string` | `varchar` |  | `not_null` | (required) Carrier Tracking Number - base language. Max length: 30 |
| `trnr__en_us` | `string` | `varchar` |  | `not_null` | (required) Tracking Number - base language. Max length: 30 |
| `refs__en_us` | `string` | `varchar` |  | `not_null` | (required) Shipment Reference - base language. Max length: 35 |
| `exre__en_us` | `string` | `varchar` |  | `not_null` | (required) Export Reference - base language. Max length: 30 |
| `erdt` | `timestamp` | `timestamp_ntz` |  |  | Export Reference Date |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `cntr` | `string` | `varchar` |  |  | Shipping Container. Max length: 4 |
| `pitm` | `string` | `varchar` |  |  | Packaging Item. Max length: 47 |
| `qpac` | `float` | `float` |  |  | Quantity of Packaging Items |
| `hght` | `float` | `float` |  |  | Height |
| `wdth` | `float` | `float` |  |  | Width |
| `dpth` | `float` | `float` |  |  | Length |
| `cdun` | `string` | `varchar` |  |  | Dimension Unit. Max length: 3 |
| `flsp` | `float` | `float` |  |  | Floor Space |
| `volm` | `float` | `float` |  |  | Volume |
| `sanl` | `integer` | `int` |  |  | Allow Changes to Shipment. Allowed values: 1, 2 |
| `sanl_kw` | `string` | `varchar` |  |  | Allow Changes to Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `srsh` | `integer` | `int` |  |  | Single Shipment Reference per Shipment. Allowed values: 1, 2 |
| `srsh_kw` | `string` | `varchar` |  |  | Single Shipment Reference per Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sdsh` | `integer` | `int` |  |  | Single Delivery Point per Shipment. Allowed values: 1, 2 |
| `sdsh_kw` | `string` | `varchar` |  |  | Single Delivery Point per Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scsh` | `integer` | `int` |  |  | Single Customer Order per Shipment. Allowed values: 1, 2 |
| `scsh_kw` | `string` | `varchar` |  |  | Single Customer Order per Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `slsh` | `integer` | `int` |  |  | Single Line per Shipment. Allowed values: 1, 2 |
| `slsh_kw` | `string` | `varchar` |  |  | Single Line per Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elsl` | `integer` | `int` |  |  | Exclude Lot in Inventory from Stock Point Consolidation in Shipment Li. Allowed values: 1, 2 |
| `elsl_kw` | `string` | `varchar` |  |  | Exclude Lot in Inventory from Stock Point Consolidation in Shipment Li (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `essl` | `integer` | `int` |  |  | Exclude Serial in Inventory from Stock Point Consolidation in Shipment. Allowed values: 1, 2 |
| `essl_kw` | `string` | `varchar` |  |  | Exclude Serial in Inventory from Stock Point Consolidation in Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eisl` | `integer` | `int` |  |  | Exclude Inventory Date from Stock Point Consolidation in Shipment Line. Allowed values: 1, 2 |
| `eisl_kw` | `string` | `varchar` |  |  | Exclude Inventory Date from Stock Point Consolidation in Shipment Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aclr` | `integer` | `int` |  |  | Additional Costs. Allowed values: 0, 10, 20, 30, 40 |
| `aclr_kw` | `string` | `varchar` |  |  | Additional Costs (keyword). Allowed values: , whadd.costs.calculated, whadd.costs.modified, whadd.costs.no, whadd.costs.not.applicable |
| `runn` | `string` | `varchar` |  |  | Run. Max length: 12 |
| `picm` | `integer` | `int` |  |  | Picking Mission |
| `eamt` | `float` | `float` |  |  | Estimated Freight Costs |
| `eacu` | `string` | `varchar` |  |  | Estimated Freight Costs Currency. Max length: 3 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `psts` | `integer` | `int` |  |  | Publishing Status. Allowed values: 10, 20, 30, 32, 34, 36, 40 |
| `psts_kw` | `string` | `varchar` |  |  | Publishing Status (keyword). Allowed values: whinh.spst.to.be.published, whinh.spst.published, whinh.spst.modified, whinh.spst.validating, whinh.spst.val.error, whinh.spst.validated, whinh.spst.not.applicable |
| `sarq` | `integer` | `int` |  |  | Source Acceptance Required. Allowed values: 1, 2 |
| `sarq_kw` | `string` | `varchar` |  |  | Source Acceptance Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sacc` | `integer` | `int` |  |  | Source Accepted. Allowed values: 1, 2 |
| `sacc_kw` | `string` | `varchar` |  |  | Source Accepted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `saby` | `string` | `varchar` |  |  | Source Acceptance by. Max length: 9 |
| `sadt` | `timestamp` | `timestamp_ntz` |  |  | Source Acceptance Date |
| `darq` | `integer` | `int` |  |  | Destination Acceptance Required. Allowed values: 1, 2 |
| `darq_kw` | `string` | `varchar` |  |  | Destination Acceptance Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dacc` | `integer` | `int` |  |  | Destination Accepted. Allowed values: 1, 2 |
| `dacc_kw` | `string` | `varchar` |  |  | Destination Accepted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `daby` | `string` | `varchar` |  |  | Destination Acceptance by. Max length: 9 |
| `dadt` | `timestamp` | `timestamp_ntz` |  |  | Destination Acceptance Date |
| `suba` | `integer` | `int` |  |  | Submitted for Acceptance. Allowed values: 1, 2 |
| `suba_kw` | `string` | `varchar` |  |  | Submitted for Acceptance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frsm` | `integer` | `int` |  |  | Freeze Mandatory. Allowed values: 1, 2 |
| `frsm_kw` | `string` | `varchar` |  |  | Freeze Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hubc` | `integer` | `int` |  |  | Handling Unit Based Confirmation. Allowed values: 1, 2 |
| `hubc_kw` | `string` | `varchar` |  |  | Handling Unit Based Confirmation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shvl` | `integer` | `int` |  |  | Shipment Validation. Allowed values: 1, 2 |
| `shvl_kw` | `string` | `varchar` |  |  | Shipment Validation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shvc` | `integer` | `int` |  |  | Shipment Validation Complete. Allowed values: 1, 2 |
| `shvc_kw` | `string` | `varchar` |  |  | Shipment Validation Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shvo` | `integer` | `int` |  |  | Shipment Validation Overruled. Allowed values: 1, 2 |
| `shvo_kw` | `string` | `varchar` |  |  | Shipment Validation Overruled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rule` | `string` | `varchar` |  |  | Validation Rule. Max length: 6 |
| `shvd` | `timestamp` | `timestamp_ntz` |  |  | Shipment Validation Date |
| `ibpd` | `integer` | `int` |  |  | Ignore Block on Package Definition Deviation. Allowed values: 1, 2 |
| `ibpd_kw` | `string` | `varchar` |  |  | Ignore Block on Package Definition Deviation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oshc` | `integer` | `int` |  |  | Overrule Shipping Constraints. Allowed values: 1, 2 |
| `oshc_kw` | `string` | `varchar` |  |  | Overrule Shipping Constraints (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sqdp` | `integer` | `int` |  |  | Sequencing during Picking. Allowed values: 10, 20, 30 |
| `sqdp_kw` | `string` | `varchar` |  |  | Sequencing during Picking (keyword). Allowed values: whinh.asds.ascending, whinh.asds.descending, whinh.asds.not.applicable |
| `sqdl` | `integer` | `int` |  |  | Sequencing during Loading. Allowed values: 10, 20, 30 |
| `sqdl_kw` | `string` | `varchar` |  |  | Sequencing during Loading (keyword). Allowed values: whinh.asds.ascending, whinh.asds.descending, whinh.asds.not.applicable |
| `psde` | `integer` | `int` |  |  | Print Shipping Documents by External Application. Allowed values: 1, 2 |
| `psde_kw` | `string` | `varchar` |  |  | Print Shipping Documents by External Application (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psdd` | `integer` | `int` |  |  | Shipping Documents Printed by External Application. Allowed values: 1, 2 |
| `psdd_kw` | `string` | `varchar` |  |  | Shipping Documents Printed by External Application (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcap` | `integer` | `int` |  |  | Localized Authorization Procedure Applicable. Allowed values: 1, 2 |
| `tcap_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tccp` | `integer` | `int` |  |  | Localized Authorization Procedure Complete. Allowed values: 1, 2 |
| `tccp_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trcd` | `string` | `varchar` |  |  | Authorization Number. Max length: 50 |
| `exfr` | `integer` | `int` |  |  | Expedited. Allowed values: 1, 2 |
| `exfr_kw` | `string` | `varchar` |  |  | Expedited (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `iedi` | `integer` | `int` |  |  | EDI Information |
| `load_cntr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh425 Shipping Containers |
| `load_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh440 Loads |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `curr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `depc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `huid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd530 Handling Units |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `prdn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh435 Delivery Notes |
| `pitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd405 Packaging Items |
| `cdun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `saby_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `daby_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `rule_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd150 Validation Rules |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `iedi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
