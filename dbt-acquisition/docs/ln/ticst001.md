# ticst001

LN ticst001 Estimated and Actual Material Costs table, Generated 2026-04-10T19:41:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ticst001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pdno`, `pono` |
| **Column count** | 224 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pdno` | `string` | `varchar` | `PK` | `not_null` | (required) Production Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `sitm` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `clot` | `string` | `varchar` |  |  | Lot Code. Max length: 20 |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `sizu` | `string` | `varchar` |  |  | Size Unit. Max length: 3 |
| `noun` | `integer` | `int` |  |  | Number of Units |
| `qune` | `float` | `float` |  |  | Net Quantity |
| `scpf` | `float` | `float` |  |  | Scrap Percentage |
| `scpq` | `float` | `float` |  |  | Scrap Quantity |
| `boun` | `string` | `varchar` |  |  | BOM Unit. Max length: 3 |
| `scdl` | `float` | `float` |  |  | Delta Scrap Quantity |
| `ques` | `float` | `float` |  |  | Estimated Quantity |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cpes_1` | `float` | `float` |  |  | Estimated Unit Cost |
| `cpes_2` | `float` | `float` |  |  | Estimated Unit Cost |
| `cpes_3` | `float` | `float` |  |  | Estimated Unit Cost |
| `cues_1` | `float` | `float` |  |  | Estimated Cost (customer owned) |
| `cues_2` | `float` | `float` |  |  | Estimated Cost (customer owned) |
| `cues_3` | `float` | `float` |  |  | Estimated Cost (customer owned) |
| `qbfd` | `float` | `float` |  |  | Quantity Backflushed |
| `qucs` | `float` | `float` |  |  | Actual Quantity |
| `qspl` | `float` | `float` |  |  | Delta Actual Split Quantity |
| `qqur` | `float` | `float` |  |  | Delta Actual Quantity to Quarantine |
| `qtor` | `float` | `float` |  |  | Quantity Rejected |
| `qscr` | `float` | `float` |  |  | Quantity Scrapped |
| `qqar` | `float` | `float` |  |  | Quantity Quarantined |
| `cpcs_1` | `float` | `float` |  |  | Actual Standard Cost |
| `cpcs_2` | `float` | `float` |  |  | Actual Standard Cost |
| `cpcs_3` | `float` | `float` |  |  | Actual Standard Cost |
| `cpcu_1` | `float` | `float` |  |  | Actual Standard Cost (customer owned) |
| `cpcu_2` | `float` | `float` |  |  | Actual Standard Cost (customer owned) |
| `cpcu_3` | `float` | `float` |  |  | Actual Standard Cost (customer owned) |
| `issu` | `float` | `float` |  |  | To Issue |
| `iswh` | `float` | `float` |  |  | To Issue by Warehousing |
| `subd` | `float` | `float` |  |  | Subsequent Delivery |
| `opno` | `integer` | `int` |  |  | Operation |
| `aldt` | `timestamp` | `timestamp_ntz` |  |  | Allocation Date |
| `bfls` | `integer` | `int` |  |  | Report Material. Allowed values: 1, 2, 30, 40, 50, 80, 90 |
| `bfls_kw` | `string` | `varchar` |  |  | Report Material (keyword). Allowed values: tirep.cons.backflush, tirep.cons.manual, tirep.cons.machine, tirep.cons.by.mes, tirep.cons.mach.group, tirep.cons.undefined, tirep.cons.not.applicable |
| `drin` | `integer` | `int` |  |  | Direct Initiate Inventory Issue. Allowed values: 1, 2 |
| `drin_kw` | `string` | `varchar` |  |  | Direct Initiate Inventory Issue (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dris` | `integer` | `int` |  |  | Direct Process Warehouse Order Line. Allowed values: 1, 2 |
| `dris_kw` | `string` | `varchar` |  |  | Direct Process Warehouse Order Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `revn` | `integer` | `int` |  |  | Obsolete |
| `mcmd` | `integer` | `int` |  |  | Material Control Method. Allowed values: 1, 2, 3, 10 |
| `mcmd_kw` | `string` | `varchar` |  |  | Material Control Method (keyword). Allowed values: tisfc.mcmd.print.and.alloc, tisfc.mcmd.print.only, tisfc.mcmd.alloc.only, tisfc.mcmd.no.control |
| `preq` | `float` | `float` |  |  | Percentage Required |
| `cctt` | `integer` | `int` |  |  | Update Method. Allowed values: 1, 2, 3 |
| `cctt_kw` | `string` | `varchar` |  |  | Update Method (keyword). Allowed values: ticst.updm.estimated, ticst.updm.actual, ticst.updm.no.action |
| `aamt_1` | `float` | `float` |  |  | Actual Amount |
| `aamt_2` | `float` | `float` |  |  | Actual Amount |
| `aamt_3` | `float` | `float` |  |  | Actual Amount |
| `aamu_1` | `float` | `float` |  |  | Actual Amount (customer owned) |
| `aamu_2` | `float` | `float` |  |  | Actual Amount (customer owned) |
| `aamu_3` | `float` | `float` |  |  | Actual Amount (customer owned) |
| `almi` | `integer` | `int` |  |  | Allow Multiple Items. Allowed values: 1, 2 |
| `almi_kw` | `string` | `varchar` |  |  | Allow Multiple Items (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `altp` | `integer` | `int` |  |  | Alternatives Present. Allowed values: 1, 2 |
| `altp_kw` | `string` | `varchar` |  |  | Alternatives Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `alfp` | `integer` | `int` |  |  | Alternative for Position |
| `rdsp` | `integer` | `int` |  |  | Reference Designators Present. Allowed values: 1, 2 |
| `rdsp_kw` | `string` | `varchar` |  |  | Reference Designators Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `alty` | `integer` | `int` |  |  | Allocation Type. Allowed values: 1, 2, 3, 4 |
| `alty_kw` | `string` | `varchar` |  |  | Allocation Type (keyword). Allowed values: tialty.none, tialty.input, tialty.output, tialty.both |
| `sbsr` | `integer` | `int` |  |  | Supplied by Subcontractor. Allowed values: 1, 2 |
| `sbsr_kw` | `string` | `varchar` |  |  | Supplied by Subcontractor (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sayn` | `integer` | `int` |  |  | Subassembly. Allowed values: 1, 2 |
| `sayn_kw` | `string` | `varchar` |  |  | Subassembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rwit` | `integer` | `int` |  |  | Rework Item. Allowed values: 1, 2 |
| `rwit_kw` | `string` | `varchar` |  |  | Rework Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bwar` | `string` | `varchar` |  |  | Original Allocation Warehouse. Max length: 6 |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `lpno` | `integer` | `int` |  |  | Linked Position |
| `rtas` | `integer` | `int` |  |  | Return as. Allowed values: 10, 20, 30, 40, 50, 90 |
| `rtas_kw` | `string` | `varchar` |  |  | Return as (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `spcn` | `string` | `varchar` |  |  | Specifications Number. Max length: 22 |
| `icfm` | `integer` | `int` |  |  | Customer Furnished Material. Allowed values: 1, 2 |
| `icfm_kw` | `string` | `varchar` |  |  | Customer Furnished Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `subc` | `integer` | `int` |  |  | Contains Customer Furnished Material. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Contains Customer Furnished Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dstp` | `integer` | `int` |  |  | Distribution Type. Allowed values: 10, 20, 30, 40, 50, 60 |
| `dstp_kw` | `string` | `varchar` |  |  | Distribution Type (keyword). Allowed values: tcdstp.not.appl, tcdstp.units, tcdstp.pegs, tcdstp.units.pegs, tcdstp.cost.pegs, tcdstp.units.cost.pegs |
| `ippg` | `integer` | `int` |  |  | Inherit Project Peg. Allowed values: 1, 2 |
| `ippg_kw` | `string` | `varchar` |  |  | Inherit Project Peg (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bmit` | `string` | `varchar` |  |  | BOM Main Item. Max length: 47 |
| `bmdl` | `string` | `varchar` |  |  | BOM Model. Max length: 9 |
| `bmrv` | `string` | `varchar` |  |  | BOM Revision. Max length: 6 |
| `bpon` | `integer` | `int` |  |  | BOM Position |
| `bseq` | `integer` | `int` |  |  | BOM Sequence |
| `effn` | `integer` | `int` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Material Text |
| `pdno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc001 Production Orders |
| `sitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `sizu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `boun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `item_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Item |
| `cpes_eam1` | `float` | `float` |  |  | CC: Estimate Unit Cost in Local Currency |
| `cpes_eam2` | `float` | `float` |  |  | CC: Estimate Unit Cost in Reporting Currency 1 |
| `cpes_eam3` | `float` | `float` |  |  | CC: Estimate Unit Cost in Reporting Currency 2 |
| `cpes_refc` | `float` | `float` |  |  | CC: Estimate Unit Cost in Reference Currency |
| `cpes_dwhc` | `float` | `float` |  |  | CC: Estimate Unit Cost in Data Warehouse Currency |
| `cues_eau1` | `float` | `float` |  |  | CC: Customer Owned Estimate Cost in Local Currency |
| `cues_eau2` | `float` | `float` |  |  | CC: Customer Owned Estimate Cost in Reporting Currency 1 |
| `cues_eau3` | `float` | `float` |  |  | CC: Customer Owned Estimate Cost in Reporting Currency 2 |
| `cues_refc` | `float` | `float` |  |  | CC: Customer Owned Estimate Cost in Reference Currency |
| `cues_dwhc` | `float` | `float` |  |  | CC: Customer Owned Estimate Unit Cost in Data Warehouse Currency |
| `cpcs_lclc` | `float` | `float` |  |  | CC: Actual Standard Cost in Local Currency |
| `cpcs_rpc1` | `float` | `float` |  |  | CC: Actual Standard Cost in Reporting Currency 1 |
| `cpcs_rpc2` | `float` | `float` |  |  | CC: Actual Standard Cost in Reporting Currency 2 |
| `cpcs_refc` | `float` | `float` |  |  | CC: Actual Standard Cost in Reference Currency |
| `cpcs_dwhc` | `float` | `float` |  |  | CC: Actual Standard Cost in Data Warehouse Currency |
| `cpcu_lclc` | `float` | `float` |  |  | CC: Customer Owned Actual Standard Cost in Local Currency |
| `cpcu_rpc1` | `float` | `float` |  |  | CC: Customer Owned Actual Standard Cost in Reporting Currency 1 |
| `cpcu_rpc2` | `float` | `float` |  |  | CC: Customer Owned Actual Standard Cost in Reporting Currency 2 |
| `cpcu_refc` | `float` | `float` |  |  | CC: Customer Owned Actual Standard Cost in Reference Currency |
| `cpcu_dwhc` | `float` | `float` |  |  | CC: Customer Owned Actual Standard Cost in Data Warehouse Currency |
| `ames_lclc` | `float` | `float` |  |  | CC: Estimate Amount in Local Currency |
| `ames_rpc1` | `float` | `float` |  |  | CC: Estimate Amount in Reporting Currency 1 |
| `ames_rpc2` | `float` | `float` |  |  | CC: Estimate Amount in Reporting Currency 2 |
| `ames_refc` | `float` | `float` |  |  | CC: Estimate Amount in Reference Currency |
| `ames_dwhc` | `float` | `float` |  |  | CC: Estimate Amount in Data Warehouse Currency |
| `aues_lclc` | `float` | `float` |  |  | CC: Customer Owned Estimate Amount in Local Currency |
| `aues_rpc1` | `float` | `float` |  |  | CC: Customer Owned Estimate Amount in Reporting Currency 1 |
| `aues_rpc2` | `float` | `float` |  |  | CC: Customer Owned Estimate Amount in Reporting Currency 2 |
| `aues_refc` | `float` | `float` |  |  | CC: Customer Owned Estimate Amount in Reference Currency |
| `aues_dwhc` | `float` | `float` |  |  | CC: Customer Owned Estimate Amount in Data Warehouse Currency |
| `aamt_lclc` | `float` | `float` |  |  | CC: Actual Amount in Local Currency |
| `aamt_rpc1` | `float` | `float` |  |  | CC: Actual Amount in Reporting Currency 1 |
| `aamt_rpc2` | `float` | `float` |  |  | CC: Actual Amount in Reporting Currency 2 |
| `aamt_refc` | `float` | `float` |  |  | CC: Actual Amount in Reference Currency |
| `aamt_dwhc` | `float` | `float` |  |  | CC: Actual Amount in Data Warehouse Currency |
| `aamu_lclc` | `float` | `float` |  |  | CC: Customer Owned Actual Amount in Local Currency |
| `aamu_rpc1` | `float` | `float` |  |  | CC: Customer Owned Actual Amount in Reporting Currency 1 |
| `aamu_rpc2` | `float` | `float` |  |  | CC: Customer Owned Actual Amount in Reporting Currency 2 |
| `aamu_refc` | `float` | `float` |  |  | CC: Customer Owned Actual Amount in Reference Currency |
| `aamu_dwhc` | `float` | `float` |  |  | CC: Customer Owned Actual Amount in Data Warehouse Currency |
| `dptm_fcmp` | `integer` | `int` |  |  | CC: Financial Company of Department |
| `scpq_buar` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Area |
| `scpq_buln` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Length |
| `scpq_bupc` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Piece |
| `scpq_butm` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Time |
| `scpq_buvl` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Volume |
| `scpq_buwg` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Weight |
| `ques_buar` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Area |
| `ques_buln` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Length |
| `ques_bupc` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Piece |
| `ques_butm` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Time |
| `ques_buvl` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Volume |
| `ques_buwg` | `float` | `float` |  |  | CC: Estimated Quantity in Base Unit Weight |
| `qucs_buar` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Area |
| `qucs_buln` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Length |
| `qucs_bupc` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Piece |
| `qucs_butm` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Time |
| `qucs_buvl` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Volume |
| `qucs_buwg` | `float` | `float` |  |  | CC: Actual Quantity in Base Unit Weight |
| `qune_buar` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Area |
| `qune_buln` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Length |
| `qune_bupc` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Piece |
| `qune_butm` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Time |
| `qune_buvl` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Volume |
| `qune_buwg` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Weight |
| `qbfd_buar` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Area |
| `qbfd_buln` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Length |
| `qbfd_bupc` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Piece |
| `qbfd_butm` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Time |
| `qbfd_buvl` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Volume |
| `qbfd_buwg` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Weight |
| `qqar_buar` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Area |
| `qqar_buln` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Length |
| `qqar_bupc` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Piece |
| `qqar_butm` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Time |
| `qqar_buvl` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Volume |
| `qqar_buwg` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Weight |
| `qtor_buar` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Area |
| `qtor_buln` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Length |
| `qtor_bupc` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Piece |
| `qtor_butm` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Time |
| `qtor_buvl` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Volume |
| `qtor_buwg` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Weight |
| `qscr_buar` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Area |
| `qscr_buln` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Length |
| `qscr_bupc` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Piece |
| `qscr_butm` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Time |
| `qscr_buvl` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Volume |
| `qscr_buwg` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Weight |
| `issu_buar` | `float` | `float` |  |  | CC: To Issue in Base Unit Area |
| `issu_buln` | `float` | `float` |  |  | CC: To Issue in Base Unit Length |
| `issu_bupc` | `float` | `float` |  |  | CC: To Issue in Base Unit Piece |
| `issu_butm` | `float` | `float` |  |  | CC: To Issue in Base Unit Time |
| `issu_buvl` | `float` | `float` |  |  | CC: To Issue in Base Unit Volume |
| `issu_buwg` | `float` | `float` |  |  | CC: To Issue in Base Unit Weight |
| `iswh_buar` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Area |
| `iswh_buln` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Length |
| `iswh_bupc` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Piece |
| `iswh_butm` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Time |
| `iswh_buvl` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Volume |
| `iswh_buwg` | `float` | `float` |  |  | CC: To Issue by Warehousing in Base Unit Weight |
| `subd_buar` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Area |
| `subd_buln` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Length |
| `subd_bupc` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Piece |
| `subd_butm` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Time |
| `subd_buvl` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Volume |
| `subd_buwg` | `float` | `float` |  |  | CC: Subsequent Delivery in Base Unit Weight |
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
