# tisfc010

LN tisfc010 Production Order Operations table, Generated 2026-04-10T19:41:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tisfc010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pdno`, `opno` |
| **Column count** | 195 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pdno` | `string` | `varchar` | `PK` | `not_null` | (required) Production Order. Max length: 9 |
| `opno` | `integer` | `int` | `PK` | `not_null` | (required) Operation |
| `item` | `string` | `varchar` |  |  | Operated Item. Max length: 47 |
| `tano` | `string` | `varchar` |  |  | Task. Max length: 8 |
| `oset` | `integer` | `int` |  |  | Operation Set |
| `sopr` | `integer` | `int` |  |  | Start Operation. Allowed values: 1, 2 |
| `sopr_kw` | `string` | `varchar` |  |  | Start Operation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nopr` | `integer` | `int` |  |  | Next Operation |
| `oopr` | `string` | `varchar` |  |  | Original Routing. Max length: 6 |
| `oopn` | `integer` | `int` |  |  | Original Operation |
| `seqn` | `integer` | `int` |  |  | Sequence Number from Routing Operation |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `mcno` | `string` | `varchar` |  |  | Machine. Max length: 6 |
| `ploc` | `string` | `varchar` |  |  | Physical Location. Max length: 15 |
| `sctt` | `integer` | `int` |  |  | Subcontracting. Allowed values: 1, 2 |
| `sctt_kw` | `string` | `varchar` |  |  | Subcontracting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ssmd` | `integer` | `int` |  |  | Subcontractor Selection Method. Allowed values: 10, 20, 30, 40 |
| `ssmd_kw` | `string` | `varchar` |  |  | Subcontractor Selection Method (keyword). Allowed values: tirou.ssmd.planning, tirou.ssmd.requisition, tirou.ssmd.manual, tirou.ssmd.not.appl |
| `bpid` | `string` | `varchar` |  |  | Subcontractor. Max length: 9 |
| `subs` | `string` | `varchar` |  |  | Subcontractor Site. Max length: 9 |
| `subr` | `float` | `float` |  |  | Subcontracting Rate Factor |
| `suba` | `string` | `varchar` |  |  | Subassembly. Max length: 47 |
| `whsa` | `string` | `varchar` |  |  | Subassembly Warehouse. Max length: 6 |
| `sset` | `integer` | `int` |  |  | Subcontracting Set |
| `sdoc` | `integer` | `int` |  |  | Subcontracting Document. Allowed values: 10, 20, 30, 40, 50 |
| `sdoc_kw` | `string` | `varchar` |  |  | Subcontracting Document (keyword). Allowed values: tisfc.sdoc.not.appl, tisfc.sdoc.required, tisfc.sdoc.pur.requisition, tisfc.sdoc.pur.order, tisfc.sdoc.on.parent |
| `tuni` | `integer` | `int` |  |  | Time Unit. Allowed values: 5, 10 |
| `tuni_kw` | `string` | `varchar` |  |  | Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `qutm` | `float` | `float` |  |  | Queue Time |
| `fxsu` | `float` | `float` |  |  | Fixed Setup Time (SCS) |
| `sstm` | `float` | `float` |  |  | Scheduled Setup Time |
| `sutm` | `float` | `float` |  |  | Average Setup Time |
| `rutm` | `float` | `float` |  |  | Cycle Time |
| `runi` | `float` | `float` |  |  | Routing Quantity |
| `prte` | `float` | `float` |  |  | Production Rate |
| `fdur` | `integer` | `int` |  |  | Fixed Duration. Allowed values: 1, 2 |
| `fdur_kw` | `string` | `varchar` |  |  | Fixed Duration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trdl` | `float` | `float` |  |  | Wait Time |
| `mvtm` | `float` | `float` |  |  | Move Time |
| `prtm` | `float` | `float` |  |  | Production Time |
| `retm` | `float` | `float` |  |  | Remaining Production Time |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transport Date To |
| `esdt` | `timestamp` | `timestamp_ntz` |  |  | Q Start Date |
| `sltm` | `float` | `float` |  |  | Slack Time |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Setup Start Date |
| `rusd` | `timestamp` | `timestamp_ntz` |  |  | Run Start Date |
| `rsdt` | `timestamp` | `timestamp_ntz` |  |  | Remainder Start Date |
| `prrd` | `timestamp` | `timestamp_ntz` |  |  | Wait Start Date |
| `fidt` | `timestamp` | `timestamp_ntz` |  |  | Move Start Date |
| `trdf` | `timestamp` | `timestamp_ntz` |  |  | Transport Date From |
| `trno` | `timestamp` | `timestamp_ntz` |  |  | Transport Date To (Next Operation) |
| `mvrd` | `timestamp` | `timestamp_ntz` |  |  | Queue Start(Next Operation) |
| `lfdt` | `timestamp` | `timestamp_ntz` |  |  | Setup (Next Oper.) |
| `fxpd` | `integer` | `int` |  |  | Fixed Planning Dates. Allowed values: 1, 2 |
| `fxpd_kw` | `string` | `varchar` |  |  | Fixed Planning Dates (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Date |
| `pfdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Finish Date |
| `bfls` | `integer` | `int` |  |  | Report Labor Hours. Allowed values: 1, 2, 30, 40, 50, 80, 90 |
| `bfls_kw` | `string` | `varchar` |  |  | Report Labor Hours (keyword). Allowed values: tirep.cons.backflush, tirep.cons.manual, tirep.cons.machine, tirep.cons.by.mes, tirep.cons.mach.group, tirep.cons.undefined, tirep.cons.not.applicable |
| `rmch` | `integer` | `int` |  |  | Report Machine Hours. Allowed values: 1, 2, 30, 40, 50, 80, 90 |
| `rmch_kw` | `string` | `varchar` |  |  | Report Machine Hours (keyword). Allowed values: tirep.cons.backflush, tirep.cons.manual, tirep.cons.machine, tirep.cons.by.mes, tirep.cons.mach.group, tirep.cons.undefined, tirep.cons.not.applicable |
| `most` | `float` | `float` |  |  | Labor Resources for Setup (FTE) |
| `mopr` | `float` | `float` |  |  | Labor Resources for Production (FTE) |
| `maho` | `float` | `float` |  |  | Labor Hours |
| `mcoc` | `float` | `float` |  |  | Machine Occupation |
| `mcho` | `float` | `float` |  |  | Machine Hours |
| `sptm` | `float` | `float` |  |  | Spent Production Time |
| `ardt` | `timestamp` | `timestamp_ntz` |  |  | Actual Ready to Start Date |
| `asdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `cctt` | `integer` | `int` |  |  | Update Method. Allowed values: 1, 2 |
| `cctt_kw` | `string` | `varchar` |  |  | Update Method (keyword). Allowed values: tisfc.cctt.planning, tisfc.cctt.report.compl |
| `opst` | `integer` | `int` |  |  | Operation Status. Allowed values: 1, 3, 4, 5, 6, 7, 8 |
| `opst_kw` | `string` | `varchar` |  |  | Operation Status (keyword). Allowed values: tisfc.opst.planned, tisfc.opst.ready.to.start, tisfc.opst.started, tisfc.opst.active, tisfc.opst.blocked, tisfc.opst.completed, tisfc.opst.closed |
| `blcd` | `string` | `varchar` |  |  | Blocking Reason. Max length: 3 |
| `copo` | `integer` | `int` |  |  | Report Product. Allowed values: 1, 2, 10, 20, 30 |
| `copo_kw` | `string` | `varchar` |  |  | Report Product (keyword). Allowed values: tirep.prod.manual, tirep.prod.automatic, tirep.prod.machine, tirep.prod.by.mes, tirep.prod.mach.group |
| `tlyn` | `integer` | `int` |  |  | Use Transfer Batch. Allowed values: 1, 2 |
| `tlyn_kw` | `string` | `varchar` |  |  | Use Transfer Batch (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trls` | `float` | `float` |  |  | Transfer Batch Qty |
| `qpli` | `float` | `float` |  |  | Quantity Planned Input |
| `qplo` | `float` | `float` |  |  | Quantity Planned Output |
| `scpq` | `float` | `float` |  |  | Setup Scrap |
| `ydtp` | `integer` | `int` |  |  | Yield Type. Allowed values: 1, 2, 3 |
| `ydtp_kw` | `string` | `varchar` |  |  | Yield Type (keyword). Allowed values: tiydtp.discrete, tiydtp.continuous, tiydtp.not.applicable |
| `yldp` | `float` | `float` |  |  | Yield Percentage |
| `qtrt` | `float` | `float` |  |  | Quantity Ratio semifinished |
| `qcmp` | `float` | `float` |  |  | Quantity Completed |
| `qtor` | `float` | `float` |  |  | Quantity Rejected |
| `qrjc` | `float` | `float` |  |  | Quantity Scrapped |
| `qxac` | `float` | `float` |  |  | Quantity Planned Scrapped |
| `qqar` | `float` | `float` |  |  | Quantity Quarantined |
| `qtbi` | `float` | `float` |  |  | Quantity to Inspect |
| `qtbf` | `float` | `float` |  |  | Quantity to Backflush |
| `qbfd` | `float` | `float` |  |  | Quantity Backflushed |
| `qsbc` | `float` | `float` |  |  | Qty Returned from Subcontracting |
| `phsp` | `float` | `float` |  |  | Phantom Scrap Quantity |
| `cont` | `string` | `varchar` |  |  | Container. Max length: 47 |
| `qpnt` | `float` | `float` |  |  | Container Quantity |
| `nnts` | `integer` | `int` |  |  | Number of Containers |
| `crem` | `string` | `varchar` |  |  | Container Method. Max length: 3 |
| `dmso` | `integer` | `int` |  |  | DMS on Completion. Allowed values: 1, 2 |
| `dmso_kw` | `string` | `varchar` |  |  | DMS on Completion (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `reco` | `string` | `varchar` |  |  | Reason for Rejects. Max length: 6 |
| `mnrs` | `integer` | `int` |  |  | Machine Operations. Allowed values: 1, 2 |
| `mnrs_kw` | `string` | `varchar` |  |  | Machine Operations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nomc` | `integer` | `int` |  |  | Number of Machines |
| `refo` | `string` | `varchar` |  |  | Reference Operation. Max length: 8 |
| `mtyp` | `string` | `varchar` |  |  | Machine Type. Max length: 9 |
| `srit` | `string` | `varchar` |  |  | Standard Routing Item. Max length: 47 |
| `orcd` | `string` | `varchar` |  |  | Original Routing Code. Max length: 9 |
| `orrv` | `string` | `varchar` |  |  | Original Routing Revision. Max length: 6 |
| `wcst` | `string` | `varchar` |  |  | Work Center Site. Max length: 9 |
| `romt` | `string` | `varchar` |  |  | Reference Operation Machine Type. Max length: 9 |
| `rosi` | `string` | `varchar` |  |  | Reference Operation Site. Max length: 9 |
| `rowc` | `string` | `varchar` |  |  | Reference Operation Work Center. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Operation Text |
| `pdno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc001 Production Orders |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tiipd001 Items - Production |
| `tano_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou003 Task |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `mcno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou002 Machine |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `subs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `suba_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `whsa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `blcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc200 Blocking Reasons |
| `reco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `mtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou460 Machine Types |
| `wcst_cwoc_mtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou461 Machine Capacity Group |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `qbfd_buar` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Area |
| `qbfd_buln` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Length |
| `qbfd_bupc` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Piece |
| `qbfd_butm` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Time |
| `qbfd_buvl` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Volume |
| `qbfd_buwg` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Weight |
| `qsbc_buar` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Area |
| `qsbc_buln` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Length |
| `qsbc_bupc` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Piece |
| `qsbc_butm` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Time |
| `qsbc_buvl` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Volume |
| `qsbc_buwg` | `float` | `float` |  |  | CC: Quantity Returned from Subcontracting in Base Unit Weight |
| `qtbf_buar` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Area |
| `qtbf_buln` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Length |
| `qtbf_bupc` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Piece |
| `qtbf_butm` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Time |
| `qtbf_buvl` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Volume |
| `qtbf_buwg` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Weight |
| `qtor_buar` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Area |
| `qtor_buln` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Length |
| `qtor_bupc` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Piece |
| `qtor_butm` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Time |
| `qtor_buvl` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Volume |
| `qtor_buwg` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Weight |
| `qtbi_buar` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Area |
| `qtbi_buln` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Length |
| `qtbi_bupc` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Piece |
| `qtbi_butm` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Time |
| `qtbi_buvl` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Volume |
| `qtbi_buwg` | `float` | `float` |  |  | CC: Quantity to Inspect in Base Unit Weight |
| `phsp_buar` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Area |
| `phsp_buln` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Length |
| `phsp_bupc` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Piece |
| `phsp_butm` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Time |
| `phsp_buvl` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Volume |
| `phsp_buwg` | `float` | `float` |  |  | CC: Phantom Scrap Quantity in Base Unit Weight |
| `qrjc_buar` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Area |
| `qrjc_buln` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Length |
| `qrjc_bupc` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Piece |
| `qrjc_butm` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Time |
| `qrjc_buvl` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Volume |
| `qrjc_buwg` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Weight |
| `qqar_buar` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Area |
| `qqar_buln` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Length |
| `qqar_bupc` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Piece |
| `qqar_butm` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Time |
| `qqar_buvl` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Volume |
| `qqar_buwg` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Weight |
| `scpq_buar` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Area |
| `scpq_buln` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Length |
| `scpq_bupc` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Piece |
| `scpq_butm` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Time |
| `scpq_buvl` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Volume |
| `scpq_buwg` | `float` | `float` |  |  | CC: Setup Scrap in Base Unit Weight |
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
