# tppdm600

LN tppdm600 Projects table, Generated 2026-04-10T19:41:59Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj` |
| **Column count** | 332 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dscb__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dscc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `padr` | `string` | `varchar` |  |  | Address Code Project. Max length: 9 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `kopr` | `integer` | `int` |  |  | Hierarchy Type. Allowed values: 1, 2, 3 |
| `kopr_kw` | `string` | `varchar` |  |  | Hierarchy Type (keyword). Allowed values: tppdm.kopr.main, tppdm.kopr.sub, tppdm.kopr.single |
| `tbdg` | `integer` | `int` |  |  | Main Project Budgeting. Allowed values: 1, 2 |
| `tbdg_kw` | `string` | `varchar` |  |  | Main Project Budgeting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `mprj` | `string` | `varchar` |  |  | Main Project. Max length: 9 |
| `pmng` | `string` | `varchar` |  |  | Project Manager. Max length: 9 |
| `agty` | `integer` | `int` |  |  | Contract Type. Allowed values: 0, 1, 2, 20 |
| `agty_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: , tppdm.agty.contract, tppdm.agty.prime.cost, tppdm.agty.na |
| `coty` | `integer` | `int` |  |  | Contract Execution. Allowed values: 0, 1, 2 |
| `coty_kw` | `string` | `varchar` |  |  | Contract Execution (keyword). Allowed values: , tppdm.coty.contract, tppdm.coty.subcontract |
| `invm` | `integer` | `int` |  |  | Invoice Type. Allowed values: 0, 1, 2, 3, 4, 5, 20 |
| `invm_kw` | `string` | `varchar` |  |  | Invoice Type (keyword). Allowed values: , tppdm.invm.prime.cost, tppdm.invm.unit.rate, tppdm.invm.instalment, tppdm.invm.inst.spec, tppdm.invm.delivery.based, tppdm.invm.na |
| `ccot` | `string` | `varchar` |  |  | Group. Max length: 3 |
| `ccam` | `string` | `varchar` |  |  | Acquiring Method. Max length: 3 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `copr` | `float` | `float` |  |  | Contract Amount |
| `pcwa` | `float` | `float` |  |  | Labor Part of Contract Amount |
| `pcba` | `float` | `float` |  |  | Labor B-Account |
| `sadr` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `rfcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Reference - base language. Max length: 15 |
| `rsip` | `string` | `varchar` |  |  | Sundry Cost Code for Provisional Amounts. Max length: 8 |
| `rsac` | `string` | `varchar` |  |  | Default Activity. Max length: 16 |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2, 3 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tppdm.prre.yes, tppdm.prre.no, tppdm.prre.stock |
| `pstf` | `integer` | `int` |  |  | Financial Result Status. Allowed values: 1, 2, 3 |
| `pstf_kw` | `string` | `varchar` |  |  | Financial Result Status (keyword). Allowed values: tppdm.pstf.free, tppdm.pstf.definite.before, tppdm.pstf.definite.after |
| `cfac` | `string` | `varchar` |  |  | Financing Method. Max length: 6 |
| `cmud` | `string` | `varchar` |  |  | Sufferance Tax. Max length: 3 |
| `pmfc` | `string` | `varchar` |  |  | Project Management Office. Max length: 6 |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `ncmp` | `integer` | `int` |  |  | Financial Company |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `plgr` | `string` | `varchar` |  |  | Planning Group. Max length: 6 |
| `psts` | `integer` | `int` |  |  | Project Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `psts_kw` | `string` | `varchar` |  |  | Project Status (keyword). Allowed values: tppdm.psts.free, tppdm.psts.construction, tppdm.psts.finished, tppdm.psts.closed, tppdm.psts.archived, tppdm.psts.deleted |
| `ptcs` | `float` | `float` |  |  | Lead Time |
| `espt` | `float` | `float` |  |  | Estimated Lead Time |
| `kptm` | `integer` | `int` |  |  | Project Lead Time Type. Allowed values: 1, 2 |
| `kptm_kw` | `string` | `varchar` |  |  | Project Lead Time Type (keyword). Allowed values: tppdm.kptm.workable.days, tppdm.kptm.calendar.days |
| `plcd` | `integer` | `int` |  |  | Planning Method. Allowed values: 1, 2 |
| `plcd_kw` | `string` | `varchar` |  |  | Planning Method (keyword). Allowed values: tcplcd.forward, tcplcd.backward |
| `cldr` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `avtp` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Time |
| `fddt` | `timestamp` | `timestamp_ntz` |  |  | Substantial Completion Date |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Finish Time |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `stus` | `integer` | `int` |  |  | Use Extensions in Budget. Allowed values: 1, 2 |
| `stus_kw` | `string` | `varchar` |  |  | Use Extensions in Budget (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `prit` | `integer` | `int` |  |  | Material Phys. Progress. Allowed values: 1, 2 |
| `prit_kw` | `string` | `varchar` |  |  | Material Phys. Progress (keyword). Allowed values: tppdm.cucc.costunit, tppdm.cucc.control |
| `prts` | `integer` | `int` |  |  | Labor Phys. Progress. Allowed values: 1, 2 |
| `prts_kw` | `string` | `varchar` |  |  | Labor Phys. Progress (keyword). Allowed values: tppdm.cucc.costunit, tppdm.cucc.control |
| `preq` | `integer` | `int` |  |  | Equipment Phys. Progress. Allowed values: 1, 2 |
| `preq_kw` | `string` | `varchar` |  |  | Equipment Phys. Progress (keyword). Allowed values: tppdm.cucc.costunit, tppdm.cucc.control |
| `prsb` | `integer` | `int` |  |  | Subcontracting Phys. Progress. Allowed values: 1, 2 |
| `prsb_kw` | `string` | `varchar` |  |  | Subcontracting Phys. Progress (keyword). Allowed values: tppdm.cucc.costunit, tppdm.cucc.control |
| `pric` | `integer` | `int` |  |  | Sundry Cost Phys. Progress. Allowed values: 1, 2 |
| `pric_kw` | `string` | `varchar` |  |  | Sundry Cost Phys. Progress (keyword). Allowed values: tppdm.cucc.costunit, tppdm.cucc.control |
| `lcdt` | `timestamp` | `timestamp_ntz` |  |  | Control Data Generation Date |
| `lclg` | `string` | `varchar` |  |  | Control Data Generated By. Max length: 16 |
| `year` | `integer` | `int` |  |  | Year Actual Cost Ctrl Period |
| `peri` | `integer` | `int` |  |  | Actual Update Period |
| `cfye` | `integer` | `int` |  |  | Closing Fiscal Year |
| `cfpr` | `integer` | `int` |  |  | Closing Fiscal Period |
| `lpdt` | `timestamp` | `timestamp_ntz` |  |  | Last History Posting Date |
| `logh` | `string` | `varchar` |  |  | User Last History Posting. Max length: 16 |
| `ccdt` | `timestamp` | `timestamp_ntz` |  |  | Cost Control Data Last Generated on Date |
| `logc` | `string` | `varchar` |  |  | User Last Control Data Generation. Max length: 16 |
| `ccal` | `string` | `varchar` |  |  | Active Budget Cost Analysis Version. Max length: 3 |
| `ratc` | `float` | `float` |  |  | Cost Rate |
| `tpac` | `string` | `varchar` |  |  | Time-Phased Budget Analysis. Max length: 3 |
| `rats` | `float` | `float` |  |  | Sales Rate |
| `ltrc` | `timestamp` | `timestamp_ntz` |  |  | Cost Rate Transaction Date |
| `ltrs` | `timestamp` | `timestamp_ntz` |  |  | Sales Rate Transaction Date |
| `btpr` | `integer` | `int` |  |  | Budget by. Allowed values: 1, 2 |
| `btpr_kw` | `string` | `varchar` |  |  | Budget by (keyword). Allowed values: tppdm.bdtp.struct.budget, tppdm.bdtp.activity.budget |
| `btco` | `integer` | `int` |  |  | Control by. Allowed values: 1, 2 |
| `btco_kw` | `string` | `varchar` |  |  | Control by (keyword). Allowed values: tppdm.bdtp.struct.budget, tppdm.bdtp.activity.budget |
| `bdmt` | `integer` | `int` |  |  | Budgeting Method. Allowed values: 1, 2 |
| `bdmt_kw` | `string` | `varchar` |  |  | Budgeting Method (keyword). Allowed values: tppdm.bdmt.production.rate, tppdm.bdmt.task.norms |
| `cspa` | `string` | `varchar` |  |  | Budget Top Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Active Plan. Max length: 3 |
| `lwta` | `integer` | `int` |  |  | Labor Rate for Actual Budget Level 1. Allowed values: 1, 2, 3, 10 |
| `lwta_kw` | `string` | `varchar` |  |  | Labor Rate for Actual Budget Level 1 (keyword). Allowed values: tppdm.lwtc.project, tppdm.lwtc.trade.group, tppdm.lwtc.task, tppdm.lwtc.not.used |
| `lwtb` | `integer` | `int` |  |  | Labor Rate for Actual Budget Level 2. Allowed values: 1, 2, 3, 10 |
| `lwtb_kw` | `string` | `varchar` |  |  | Labor Rate for Actual Budget Level 2 (keyword). Allowed values: tppdm.lwtc.project, tppdm.lwtc.trade.group, tppdm.lwtc.task, tppdm.lwtc.not.used |
| `lwtc` | `integer` | `int` |  |  | Labor Rate for Actual Budget Level 3. Allowed values: 1, 2, 3, 10 |
| `lwtc_kw` | `string` | `varchar` |  |  | Labor Rate for Actual Budget Level 3 (keyword). Allowed values: tppdm.lwtc.project, tppdm.lwtc.trade.group, tppdm.lwtc.task, tppdm.lwtc.not.used |
| `lwha` | `integer` | `int` |  |  | Labor Rate for Hours Accounting Level 1. Allowed values: 1, 2, 3, 4, 5, 10 |
| `lwha_kw` | `string` | `varchar` |  |  | Labor Rate for Hours Accounting Level 1 (keyword). Allowed values: tppdm.lwha.project, tppdm.lwha.trade.group, tppdm.lwha.task, tppdm.lwha.employee, tppdm.lwha.department, tppdm.lwha.not.used |
| `lwhb` | `integer` | `int` |  |  | Labor Rate for Hours Accounting Level 2. Allowed values: 1, 2, 3, 4, 5, 10 |
| `lwhb_kw` | `string` | `varchar` |  |  | Labor Rate for Hours Accounting Level 2 (keyword). Allowed values: tppdm.lwha.project, tppdm.lwha.trade.group, tppdm.lwha.task, tppdm.lwha.employee, tppdm.lwha.department, tppdm.lwha.not.used |
| `lwhc` | `integer` | `int` |  |  | Labor Rate for Hours Accounting Level 3. Allowed values: 1, 2, 3, 4, 5, 10 |
| `lwhc_kw` | `string` | `varchar` |  |  | Labor Rate for Hours Accounting Level 3 (keyword). Allowed values: tppdm.lwha.project, tppdm.lwha.trade.group, tppdm.lwha.task, tppdm.lwha.employee, tppdm.lwha.department, tppdm.lwha.not.used |
| `lwhd` | `integer` | `int` |  |  | Labor Rate for Hours Accounting Level 4. Allowed values: 1, 2, 3, 4, 5, 10 |
| `lwhd_kw` | `string` | `varchar` |  |  | Labor Rate for Hours Accounting Level 4 (keyword). Allowed values: tppdm.lwha.project, tppdm.lwha.trade.group, tppdm.lwha.task, tppdm.lwha.employee, tppdm.lwha.department, tppdm.lwha.not.used |
| `wipc` | `float` | `float` |  |  | WIP Sales Value Surcharge |
| `pcit` | `float` | `float` |  |  | Material Surcharge Percentage |
| `pcts` | `float` | `float` |  |  | Labor Surcharge Percentage |
| `pceq` | `float` | `float` |  |  | Equipment Surcharge Percentage |
| `pcsb` | `float` | `float` |  |  | Subcontracting Surcharge Percentage |
| `pcic` | `float` | `float` |  |  | Sundry Cost Surcharge Percentage |
| `cmog` | `integer` | `int` |  |  | Collection Method Requirements Planning. Allowed values: 1, 2, 3 |
| `cmog_kw` | `string` | `varchar` |  |  | Collection Method Requirements Planning (keyword). Allowed values: tppdm.cmog.project, tppdm.cmog.structure.part, tppdm.cmog.activity |
| `njsp` | `integer` | `int` |  |  | Number of Job Sheets Printed |
| `psit` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Suppliers (Mat.) |
| `psta` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Suppliers (Labor) |
| `pseq` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Suppliers (Equipment) |
| `pssc` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Suppliers (Subcontracting) |
| `psic` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Suppliers (Sundry Cost) |
| `rsrv` | `integer` | `int` |  |  | Payment Terms for Financial Analysis Cust. |
| `dwar` | `string` | `varchar` |  |  | Default Project Warehouse. Max length: 6 |
| `pwar` | `string` | `varchar` |  |  | Priority Supply Warehouse. Max length: 6 |
| `ccur` | `string` | `varchar` |  |  | Project Currency. Max length: 3 |
| `uobs` | `integer` | `int` |  |  | Use Organization Breakdown Structure. Allowed values: 1, 2 |
| `uobs_kw` | `string` | `varchar` |  |  | Use Organization Breakdown Structure (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cobs` | `string` | `varchar` |  |  | Organization Breakdown Structure. Max length: 9 |
| `btdt` | `timestamp` | `timestamp_ntz` |  |  | Budget Date |
| `invr` | `integer` | `int` |  |  | Inventory Reservation. Allowed values: 1, 2, 3 |
| `invr_kw` | `string` | `varchar` |  |  | Inventory Reservation (keyword). Allowed values: tppdm.invr.insert, tppdm.invr.confirm, tppdm.invr.none |
| `hbpc` | `float` | `float` |  |  | Holdback Percentage |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type Budget. Max length: 3 |
| `ussu` | `integer` | `int` |  |  | Use Surcharges. Allowed values: 1, 2, 3, 4 |
| `ussu_kw` | `string` | `varchar` |  |  | Use Surcharges (keyword). Allowed values: tppdm.ussu.surch.no, tppdm.ussu.surch.budget, tppdm.ussu.surch.actual, tppdm.ussu.surch.all |
| `intp` | `integer` | `int` |  |  | Internal Project. Allowed values: 1, 2 |
| `intp_kw` | `string` | `varchar` |  |  | Internal Project (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `capp` | `integer` | `int` |  |  | Capital Project. Allowed values: 1, 2 |
| `capp_kw` | `string` | `varchar` |  |  | Capital Project (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `aext` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `anbr` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `ipin` | `integer` | `int` |  |  | Invoicing via PIN. Allowed values: 1, 2 |
| `ipin_kw` | `string` | `varchar` |  |  | Invoicing via PIN (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `crtp` | `string` | `varchar` |  |  | Exchange Rate Type Costs. Max length: 3 |
| `rrtp` | `string` | `varchar` |  |  | Exchange Rate Type Revenues. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rcmc_1` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_2` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_3` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `cuni` | `string` | `varchar` |  |  | Rate Unit. Max length: 3 |
| `rsmc_1` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_2` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_3` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rjct` | `integer` | `int` |  |  | Rejected. Allowed values: 1, 2 |
| `rjct_kw` | `string` | `varchar` |  |  | Rejected (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ofes` | `string` | `varchar` |  |  | Originated from Estimate. Max length: 9 |
| `ripr` | `string` | `varchar` |  |  | Resulted in Project. Max length: 9 |
| `cdis` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `lcla` | `string` | `varchar` |  |  | Last Launched by. Max length: 16 |
| `dlau` | `timestamp` | `timestamp_ntz` |  |  | Date of Launch |
| `upal` | `integer` | `int` |  |  | Update Allowed. Allowed values: 1, 2 |
| `upal_kw` | `string` | `varchar` |  |  | Update Allowed (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Primary Scenario Description - base language. Max length: 30 |
| `revr` | `integer` | `int` |  |  | Revenue Recognition Method. Allowed values: 1, 2, 3, 4, 5, 6, 20 |
| `revr_kw` | `string` | `varchar` |  |  | Revenue Recognition Method (keyword). Allowed values: tppdm.revr.cmpl, tppdm.revr.perc, tppdm.revr.mils, tppdm.revr.rebu, tppdm.revr.erfc, tppdm.revr.actr, tppdm.revr.na |
| `cmpc` | `integer` | `int` |  |  | Calculation Method for Percentage of Completion. Allowed values: 10, 20, 100, 200 |
| `cmpc_kw` | `string` | `varchar` |  |  | Calculation Method for Percentage of Completion (keyword). Allowed values: tppdm.cmpc.cost.to.cost, tppdm.cmpc.hours.progress, tppdm.cmpc.manually, tppdm.cmpc.na |
| `pcmp` | `float` | `float` |  |  | Percentage of Completion |
| `bfrf` | `integer` | `int` |  |  | Base for Earned Revenue Factor. Allowed values: 10, 20, 30, 100 |
| `bfrf_kw` | `string` | `varchar` |  |  | Base for Earned Revenue Factor (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `earf` | `float` | `float` |  |  | Earned Revenue Factor |
| `revt` | `float` | `float` |  |  | Revenue Recognition Threshold |
| `revl` | `float` | `float` |  |  | Revenue Recognition Limit |
| `cogs` | `integer` | `int` |  |  | Costs of Sales Method. Allowed values: 1, 2, 3, 10, 20 |
| `cogs_kw` | `string` | `varchar` |  |  | Costs of Sales Method (keyword). Allowed values: tppdm.cogs.cmpl, tppdm.cogs.prop, tppdm.cogs.rebu, tppdm.cogs.perc, tppdm.cogs.na |
| `cogc` | `float` | `float` |  |  | Cost of Sales Percentage of Completion |
| `bfpp` | `integer` | `int` |  |  | Base for Profit Percentage. Allowed values: 10, 20, 30, 100 |
| `bfpp_kw` | `string` | `varchar` |  |  | Base for Profit Percentage (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `prpc` | `float` | `float` |  |  | Profit Percentage |
| `ascd__en_us` | `string` | `varchar` |  | `not_null` | (required) Alternate Scenario Description - base language. Max length: 30 |
| `arrm` | `integer` | `int` |  |  | Alternate Revenue Recognition Method. Allowed values: 1, 2, 3, 4, 5, 6, 20 |
| `arrm_kw` | `string` | `varchar` |  |  | Alternate Revenue Recognition Method (keyword). Allowed values: tppdm.revr.cmpl, tppdm.revr.perc, tppdm.revr.mils, tppdm.revr.rebu, tppdm.revr.erfc, tppdm.revr.actr, tppdm.revr.na |
| `acmp` | `integer` | `int` |  |  | Alternate Calculation Method for Percentage of Completion. Allowed values: 10, 20, 100, 200 |
| `acmp_kw` | `string` | `varchar` |  |  | Alternate Calculation Method for Percentage of Completion (keyword). Allowed values: tppdm.cmpc.cost.to.cost, tppdm.cmpc.hours.progress, tppdm.cmpc.manually, tppdm.cmpc.na |
| `arpc` | `float` | `float` |  |  | Alternate Revenue Percentage of Completion |
| `abrf` | `integer` | `int` |  |  | Alternate Base for Earned Revenue Factor. Allowed values: 10, 20, 30, 100 |
| `abrf_kw` | `string` | `varchar` |  |  | Alternate Base for Earned Revenue Factor (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `aerf` | `float` | `float` |  |  | Alternate Earned Revenue Factor |
| `arrt` | `float` | `float` |  |  | Alternate Revenue Recognition Threshold |
| `arrl` | `float` | `float` |  |  | Alternate Revenue Recognition Limit |
| `acgm` | `integer` | `int` |  |  | Alternate Cost of Sales Method. Allowed values: 1, 2, 3, 10, 20 |
| `acgm_kw` | `string` | `varchar` |  |  | Alternate Cost of Sales Method (keyword). Allowed values: tppdm.cogs.cmpl, tppdm.cogs.prop, tppdm.cogs.rebu, tppdm.cogs.perc, tppdm.cogs.na |
| `acpc` | `float` | `float` |  |  | Alternate Cost of Sales Percentage of Completion |
| `abpp` | `integer` | `int` |  |  | Alternate Base for Profit Percentage. Allowed values: 10, 20, 30, 100 |
| `abpp_kw` | `string` | `varchar` |  |  | Alternate Base for Profit Percentage (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `aprp` | `float` | `float` |  |  | Alternate Profit Percentage |
| `tmpl` | `integer` | `int` |  |  | Template. Allowed values: 1, 2 |
| `tmpl_kw` | `string` | `varchar` |  |  | Template (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pcrd` | `timestamp` | `timestamp_ntz` |  |  | Project Creation Date |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `ofpr` | `string` | `varchar` |  |  | Originated from Project. Max length: 9 |
| `pror` | `integer` | `int` |  |  | Project Origin. Allowed values: 1, 2, 3, 4, 5, 6 |
| `pror_kw` | `string` | `varchar` |  |  | Project Origin (keyword). Allowed values: tppdm.pror.na, tppdm.pror.project, tppdm.pror.template, tppdm.pror.estimate, tppdm.pror.extern, tppdm.pror.manual |
| `prgm` | `string` | `varchar` |  |  | Program. Max length: 30 |
| `potp` | `string` | `varchar` |  |  | Project Procedure. Max length: 3 |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `guid` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `inst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `cprq` | `integer` | `int` |  |  | Combine Planned Requirements on. Allowed values: 10, 20, 30 |
| `cprq_kw` | `string` | `varchar` |  |  | Combine Planned Requirements on (keyword). Allowed values: tppdm.cprq.project, tppdm.cprq.structure, tppdm.cprq.budget.line |
| `habo` | `integer` | `int` |  |  | Has Activity Budget Overview. Allowed values: 1, 2 |
| `habo_kw` | `string` | `varchar` |  |  | Has Activity Budget Overview (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `hebo` | `integer` | `int` |  |  | Has Element Budget Overview. Allowed values: 1, 2 |
| `hebo_kw` | `string` | `varchar` |  |  | Has Element Budget Overview (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modified on |
| `igcd` | `integer` | `int` |  |  | Include Generic Items in Control Data. Allowed values: 1, 2 |
| `igcd_kw` | `string` | `varchar` |  |  | Include Generic Items in Control Data (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `edbg` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `bqua` | `float` | `float` |  |  | Obsolete |
| `pcbq` | `float` | `float` |  |  | Obsolete |
| `rdbg` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `rbqu` | `float` | `float` |  |  | Obsolete |
| `rpcb` | `float` | `float` |  |  | Obsolete |
| `dacd` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `decd` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `dpcd` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `cdf_acnt` | `string` | `varchar` |  |  | Project Accountant. Max length: 9 |
| `cdf_acrb` | `float` | `float` |  |  | Total Project Accrual Balance |
| `cdf_ampc` | `string` | `varchar` |  |  | AMP Code. Max length: 16 |
| `cdf_ampd` | `string` | `varchar` |  |  | AMP Code Name. Max length: 80 |
| `cdf_awip` | `float` | `float` |  |  | WIP Balance |
| `cdf_bcpc` | `integer` | `int` |  |  | Business Case Process. Allowed values: 1, 2, 3 |
| `cdf_bcpc_kw` | `string` | `varchar` |  |  | Business Case Process (keyword). Allowed values: tpcdf_lst007.b, tpcdf_lst007.initiate, tpcdf_lst007.initiated |
| `cdf_camt` | `float` | `float` |  |  | Commitment to Date |
| `cdf_cpin` | `integer` | `int` |  |  | Capitalised Interest. Allowed values: 1, 2 |
| `cdf_cpin_kw` | `string` | `varchar` |  |  | Capitalised Interest (keyword). Allowed values: tpcdf______chk.yes, tpcdf______chk.no |
| `cdf_fwcc` | `integer` | `int` |  |  | Forward Works Category. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 |
| `cdf_fwcc_kw` | `string` | `varchar` |  |  | Forward Works Category (keyword). Allowed values: tpcdf_lst001.a, tpcdf_lst001.b, tpcdf_lst001.c, tpcdf_lst001.d, tpcdf_lst001.e, tpcdf_lst001.f, tpcdf_lst001.g, tpcdf_lst001.h, tpcdf_lst001.i, tpcdf_lst001.j, tpcdf_lst001.k, tpcdf_lst001.l, tpcdf_lst001.m, tpcdf_lst001.n, tpcdf_lst001.o, tpcdf_lst001.na, tpcdf_lst001.p, tpcdf_lst001.q, tpcdf_lst001.st, tpcdf_lst001.slt, tpcdf_lst001.rc, tpcdf_lst001.wmt, tpcdf_lst001.ewtp, tpcdf_lst001.ewwtp, tpcdf_lst001.ewn, tpcdf_lst001.ewwn, tpcdf_lst001.blank |
| `cdf_fwdc` | `integer` | `int` |  |  | Forward Works Description |
| `cdf_fwic` | `integer` | `int` |  |  | Forward Works Include. Allowed values: 1, 2, 3 |
| `cdf_fwic_kw` | `string` | `varchar` |  |  | Forward Works Include (keyword). Allowed values: tpcdf_lst004.y, tpcdf_lst004.n, tpcdf_lst004.na |
| `cdf_fwpc__en_us` | `string` | `varchar` |  | `not_null` | (required) Forward Works Programme - base language. Max length: 189 |
| `cdf_grow` | `integer` | `int` |  |  | Growth % |
| `cdf_locn` | `integer` | `int` |  |  | Location. Allowed values: 1, 2, 3 |
| `cdf_locn_kw` | `string` | `varchar` |  |  | Location (keyword). Allowed values: tpcdf_lst005.n, tpcdf_lst005.s, tpcdf_lst005.na |
| `cdf_lprj` | `string` | `varchar` |  |  | Legacy Project. Max length: 50 |
| `cdf_lser` | `integer` | `int` |  |  | Level of Service % |
| `cdf_pacd` | `timestamp` | `timestamp_ntz` |  |  | System Project Closure Date |
| `cdf_pnam__en_us` | `string` | `varchar` |  | `not_null` | (required) Project Name - base language. Max length: 50 |
| `cdf_prjc` | `integer` | `int` |  |  | Project Control Commentary. Allowed values: 1, 2 |
| `cdf_prjc_kw` | `string` | `varchar` |  |  | Project Control Commentary (keyword). Allowed values: tpcdf______chk.yes, tpcdf______chk.no |
| `cdf_prju` | `integer` | `int` |  |  | Project Update |
| `cdf_renw` | `integer` | `int` |  |  | Renewal % |
| `cdf_sdtl` | `integer` | `int` |  |  | Service Delivery Traffic Light. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_sdtl_kw` | `string` | `varchar` |  |  | Service Delivery Traffic Light (keyword). Allowed values: tpcdf_lst006.g, tpcdf_lst006.o, tpcdf_lst006.r, tpcdf_lst006.na, tpcdf_lst006.confirm, tpcdf_lst006.dg |
| `cdf_sqtl` | `integer` | `int` |  |  | Scope/Quality Traffic Light. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_sqtl_kw` | `string` | `varchar` |  |  | Scope/Quality Traffic Light (keyword). Allowed values: tpcdf_lst006.g, tpcdf_lst006.o, tpcdf_lst006.r, tpcdf_lst006.na, tpcdf_lst006.confirm, tpcdf_lst006.dg |
| `cdf_svcc` | `integer` | `int` |  |  | Spend Variance Commentary. Allowed values: 1, 2 |
| `cdf_svcc_kw` | `string` | `varchar` |  |  | Spend Variance Commentary (keyword). Allowed values: tpcdf______chk.yes, tpcdf______chk.no |
| `cdf_tcac` | `float` | `float` |  |  | Carbon Actual |
| `cdf_tcbl` | `float` | `float` |  |  | Carbon Baseline |
| `cdf_tcfc` | `float` | `float` |  |  | Carbon Forecast |
| `cdf_tcfl` | `integer` | `int` |  |  | Carbon Forecast Locked. Allowed values: 1, 2 |
| `cdf_tcfl_kw` | `string` | `varchar` |  |  | Carbon Forecast Locked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_tflc` | `integer` | `int` |  |  | Traffic Light Commentary. Allowed values: 1, 2 |
| `cdf_tflc_kw` | `string` | `varchar` |  |  | Traffic Light Commentary (keyword). Allowed values: tpcdf______chk.yes, tpcdf______chk.no |
| `cdf_tlct` | `integer` | `int` |  |  | Traffic Light Commentary |
| `cdf_tmtl` | `integer` | `int` |  |  | Time/Milestone Traffic Light. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_tmtl_kw` | `string` | `varchar` |  |  | Time/Milestone Traffic Light (keyword). Allowed values: tpcdf_lst006.g, tpcdf_lst006.o, tpcdf_lst006.r, tpcdf_lst006.na, tpcdf_lst006.confirm, tpcdf_lst006.dg |
| `cdf_trfl` | `integer` | `int` |  |  | Baseline Date/Forecast Date Traffic light. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_trfl_kw` | `string` | `varchar` |  |  | Baseline Date/Forecast Date Traffic light (keyword). Allowed values: tpcdf_lst006.g, tpcdf_lst006.o, tpcdf_lst006.r, tpcdf_lst006.na, tpcdf_lst006.confirm, tpcdf_lst006.dg |
| `cdf_uamt` | `float` | `float` |  |  | Unapproved Commitment |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc300 Budget Cost Analysis Versions by Project |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_cpla_rsac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `padr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pmng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm065 Project Groups |
| `ccam_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm063 Acquiring Methods |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `sadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm059 Financing Methods |
| `cmud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm057 Sufferance Tax |
| `pmfc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm010 Project Management Office |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `plgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcpeg001 Planning Groups |
| `cldr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `avtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `dwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `pwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cobs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm095 User Defined Structures |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `crtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `rrtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ofes_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ripr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ofpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `prgm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm010 Programs |
| `potp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm680 Project Procedures |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `cdf_acnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cdf_ampc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table txpdm909 AMP Codes |
| `cdf_fwdc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_prju_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_tlct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `entu_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Enterprise Unit |
| `pmfc_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Project Management Office |
| `mprj_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Main Project |
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
