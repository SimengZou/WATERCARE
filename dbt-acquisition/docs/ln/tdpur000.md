# tdpur000

LN tdpur000 Procurement Parameters table, Generated 2019-05-10T10:44:27Z from package combination 1060016

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur000` |
| **Materialization** | `incremental` |
| **Column count** | 483 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` |  | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Effective Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `rqsi` | `integer` | `int` |  |  | Requisitions Module Implemented. Allowed values: 1, 2 |
| `rqsi_kw` | `string` | `varchar` |  |  | Requisitions Module Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rfqi` | `integer` | `int` |  |  | Requests for Quotation Module Implemented. Allowed values: 1, 2 |
| `rfqi_kw` | `string` | `varchar` |  |  | Requests for Quotation Module Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `conp_3` | `integer` | `int` |  |  | Contracts Module Implemented. Allowed values: 1, 2 |
| `conp_3_kw` | `string` | `varchar` |  |  | Contracts Module Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sdli` | `integer` | `int` |  |  | Schedules Module Implemented. Allowed values: 1, 2 |
| `sdli_kw` | `string` | `varchar` |  |  | Schedules Module Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vdri` | `integer` | `int` |  |  | Vendor Rating Implemented. Allowed values: 1, 2 |
| `vdri_kw` | `string` | `varchar` |  |  | Vendor Rating Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tpim` | `integer` | `int` |  |  | Target Price Implemented. Allowed values: 1, 2 |
| `tpim_kw` | `string` | `varchar` |  |  | Target Price Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `puom` | `string` | `varchar` |  |  | Purchase Order Unit of Measure. Max length: 3 |
| `citt` | `string` | `varchar` |  |  | Default Alternative Item Code System. Max length: 3 |
| `lpof` | `integer` | `int` |  |  | Log Financial Economic Transactions. Allowed values: 1, 2, 3 |
| `lpof_kw` | `string` | `varchar` |  |  | Log Financial Economic Transactions (keyword). Allowed values: tdpur.lpof.no, tdpur.lpof.orderprice, tdpur.lpof.costprice |
| `nbol` | `integer` | `int` |  |  | Maximum Number of Phantom Levels to be Skipped |
| `clgr` | `string` | `varchar` |  |  | List Group Purchase. Max length: 3 |
| `disf` | `integer` | `int` |  |  | Insertion of Default Item - Purchase Business Partner. Allowed values: 1, 2 |
| `disf_kw` | `string` | `varchar` |  |  | Insertion of Default Item - Purchase Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ract` | `string` | `varchar` |  |  | Default Availability Type. Max length: 6 |
| `txct` | `integer` | `int` |  |  | Print Tax Text. Allowed values: 1, 2 |
| `txct_kw` | `string` | `varchar` |  |  | Print Tax Text (keyword). Allowed values: tdcopt.mandatory, tdcopt.optional |
| `codt` | `integer` | `int` |  |  | Configuration Date. Allowed values: 1, 2, 3 |
| `codt_kw` | `string` | `varchar` |  |  | Configuration Date (keyword). Allowed values: tdpur.pric.order.date, tdpur.pric.system.date, tdpur.pric.delivery.date |
| `glcu` | `integer` | `int` |  |  | Use General Ledger Codes for Ledger Account Entry. Allowed values: 1, 2 |
| `glcu_kw` | `string` | `varchar` |  |  | Use General Ledger Codes for Ledger Account Entry (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `glcm` | `integer` | `int` |  |  | General Ledger Codes Mandatory. Allowed values: 1, 2 |
| `glcm_kw` | `string` | `varchar` |  |  | General Ledger Codes Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fstu` | `integer` | `int` |  |  | Use Full Supply Time. Allowed values: 1, 2 |
| `fstu_kw` | `string` | `varchar` |  |  | Use Full Supply Time (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prmp` | `integer` | `int` |  |  | Pricing by MPN. Allowed values: 1, 2 |
| `prmp_kw` | `string` | `varchar` |  |  | Pricing by MPN (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `impn` | `integer` | `int` |  |  | Multiple Items per MPN. Allowed values: 1, 2 |
| `impn_kw` | `string` | `varchar` |  |  | Multiple Items per MPN (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pegy` | `integer` | `int` |  |  | Always Project Peg Cost Items without Warehouse. Allowed values: 1, 2 |
| `pegy_kw` | `string` | `varchar` |  |  | Always Project Peg Cost Items without Warehouse (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rbim` | `integer` | `int` |  |  | Retro-Billing Implemented. Allowed values: 1, 2 |
| `rbim_kw` | `string` | `varchar` |  |  | Retro-Billing Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rbng` | `string` | `varchar` |  |  | Number Group for Retro-Billing Advices. Max length: 3 |
| `rbsr` | `string` | `varchar` |  |  | Series for Retro-Billing Advices. Max length: 8 |
| `cald` | `integer` | `int` |  |  | Date for Generating Retro-Billed Advice. Allowed values: 1, 2, 3 |
| `cald_kw` | `string` | `varchar` |  |  | Date for Generating Retro-Billed Advice (keyword). Allowed values: tdsls.cald.delivery, tdsls.cald.transaction, tdsls.cald.invoice |
| `rbap` | `integer` | `int` |  |  | Retro-Billing Cost/Service. Allowed values: 1, 2, 3, 4 |
| `rbap_kw` | `string` | `varchar` |  |  | Retro-Billing Cost/Service (keyword). Allowed values: tdsls.rbap.cost, tdsls.rbap.service, tdsls.rbap.both, tdsls.rbap.none |
| `hisq_1` | `integer` | `int` |  |  | Log RFQ History. Allowed values: 1, 2 |
| `hisq_1_kw` | `string` | `varchar` |  |  | Log RFQ History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arrh_1` | `integer` | `int` |  |  | RFQ History Archiving Implemented. Allowed values: 1, 2 |
| `arrh_1_kw` | `string` | `varchar` |  |  | RFQ History Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rpds_1` | `integer` | `int` |  |  | Default Term for Returning RFQ's |
| `pqcp_1` | `integer` | `int` |  |  | Number of Extra RFQ Copies |
| `rmcp_1` | `integer` | `int` |  |  | Number of Extra RFQ Reminder Copies |
| `ngpi_1` | `string` | `varchar` |  |  | Number Group for RFQs. Max length: 3 |
| `sspi_1` | `integer` | `int` |  |  | Step Size for RFQs |
| `capo_1` | `integer` | `int` |  |  | Check RFQ on Actual Purchase Orders. Allowed values: 1, 2 |
| `capo_1_kw` | `string` | `varchar` |  |  | Check RFQ on Actual Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crfq_1` | `integer` | `int` |  |  | Check RFQ on Actual Requisitions. Allowed values: 1, 2 |
| `crfq_1_kw` | `string` | `varchar` |  |  | Check RFQ on Actual Requisitions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccon_1` | `integer` | `int` |  |  | Check RFQ on Actual Contracts. Allowed values: 1, 2 |
| `ccon_1_kw` | `string` | `varchar` |  |  | Check RFQ on Actual Contracts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csch_1` | `integer` | `int` |  |  | Check RFQ on Actual Schedules. Allowed values: 1, 2 |
| `csch_1_kw` | `string` | `varchar` |  |  | Check RFQ on Actual Schedules (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `snas_l` | `integer` | `int` |  |  | Send RFQ to Not approved Buy-from Business Partner. Allowed values: 1, 2 |
| `snas_l_kw` | `string` | `varchar` |  |  | Send RFQ to Not approved Buy-from Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cset_1` | `string` | `varchar` |  |  | RFQ Criteria Set. Max length: 6 |
| `psmn_1` | `integer` | `int` |  |  | Price Stage Mandatory for RFQs. Allowed values: 1, 2 |
| `psmn_1_kw` | `string` | `varchar` |  |  | Price Stage Mandatory for RFQs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `enlh_1` | `integer` | `int` |  |  | Enhanced Line Handling. Allowed values: 1, 2 |
| `enlh_1_kw` | `string` | `varchar` |  |  | Enhanced Line Handling (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eint_1` | `integer` | `int` |  |  | External Integration. Allowed values: 1, 2 |
| `eint_1_kw` | `string` | `varchar` |  |  | External Integration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `borl_1` | `integer` | `int` |  |  | Buyer on RFQ Line. Allowed values: 1, 2 |
| `borl_1_kw` | `string` | `varchar` |  |  | Buyer on RFQ Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngpr_2` | `string` | `varchar` |  |  | Number Group for Requisitions. Max length: 3 |
| `sspr_2` | `integer` | `int` |  |  | Step Size for Purchase Requisitions |
| `capo_2` | `integer` | `int` |  |  | Check Requisition on Actual Purchase Orders. Allowed values: 1, 2 |
| `capo_2_kw` | `string` | `varchar` |  |  | Check Requisition on Actual Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crfq_2` | `integer` | `int` |  |  | Check Requisition on Actual RFQs. Allowed values: 1, 2 |
| `crfq_2_kw` | `string` | `varchar` |  |  | Check Requisition on Actual RFQs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `borl_2` | `integer` | `int` |  |  | Buyer on Requisition Line. Allowed values: 1, 2 |
| `borl_2_kw` | `string` | `varchar` |  |  | Buyer on Requisition Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hisr_2` | `integer` | `int` |  |  | Log Requisition History. Allowed values: 1, 2, 3 |
| `hisr_2_kw` | `string` | `varchar` |  |  | Log Requisition History (keyword). Allowed values: tdpur.hist.no.logging, tdpur.hist.only.last, tdpur.hist.all.changes |
| `arrh_2` | `integer` | `int` |  |  | Requisition History Archiving Implemented. Allowed values: 1, 2 |
| `arrh_2_kw` | `string` | `varchar` |  |  | Requisition History Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lpof_2` | `integer` | `int` |  |  | Log Financial Economic Transactions for Requisitions. Allowed values: 1, 2 |
| `lpof_2_kw` | `string` | `varchar` |  |  | Log Financial Economic Transactions for Requisitions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `apau_2` | `integer` | `int` |  |  | Approval Authorizations. Allowed values: 1, 2, 3 |
| `apau_2_kw` | `string` | `varchar` |  |  | Approval Authorizations (keyword). Allowed values: tdpur.auth.any.appr, tdpur.auth.any.appr.of.dep, tdpur.auth.only.curr.appr |
| `rssu_2` | `string` | `varchar` |  |  | Requisition Series for Subcontracting. Max length: 8 |
| `rsss_2` | `string` | `varchar` |  |  | Requisition Series for Service Subcontracting. Max length: 8 |
| `sgra_2` | `integer` | `int` |  |  | Submit Generated Requisitions Automatically. Allowed values: 1, 2 |
| `sgra_2_kw` | `string` | `varchar` |  |  | Submit Generated Requisitions Automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngco_3` | `string` | `varchar` |  |  | Number Group for Call-offs. Max length: 3 |
| `ngpc_3` | `string` | `varchar` |  |  | Number Group for Purchase Contracts. Max length: 3 |
| `ngsr_3` | `string` | `varchar` |  |  | Number Group for Releases. Max length: 3 |
| `ucpc_3` | `integer` | `int` |  |  | Use Corporate Purchase Contracts. Allowed values: 1, 2 |
| `ucpc_3_kw` | `string` | `varchar` |  |  | Use Corporate Purchase Contracts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cabt_3` | `integer` | `int` |  |  | Maintaining Contracts Always Allowed. Allowed values: 1, 2 |
| `cabt_3_kw` | `string` | `varchar` |  |  | Maintaining Contracts Always Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cppy_3` | `integer` | `int` |  |  | Copy Special Contract to Normal Contract. Allowed values: 1, 2 |
| `cppy_3_kw` | `string` | `varchar` |  |  | Copy Special Contract to Normal Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpyp_3` | `integer` | `int` |  |  | Copy Normal Contract to Special Contract. Allowed values: 1, 2 |
| `cpyp_3_kw` | `string` | `varchar` |  |  | Copy Normal Contract to Special Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ndbe_3` | `integer` | `int` |  |  | Evaluate Contract before Deleting. Allowed values: 1, 2 |
| `ndbe_3_kw` | `string` | `varchar` |  |  | Evaluate Contract before Deleting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hish_3` | `integer` | `int` |  |  | Log Contract Transactions. Allowed values: 1, 2, 3 |
| `hish_3_kw` | `string` | `varchar` |  |  | Log Contract Transactions (keyword). Allowed values: tdpur.hist.no.logging, tdpur.hist.only.last, tdpur.hist.all.changes |
| `hisl_3` | `integer` | `int` |  |  | Log Contract Line Transactions. Allowed values: 1, 2, 3 |
| `hisl_3_kw` | `string` | `varchar` |  |  | Log Contract Line Transactions (keyword). Allowed values: tdpur.hist.no.logging, tdpur.hist.only.last, tdpur.hist.all.changes |
| `hist_3` | `integer` | `int` |  |  | Log Turnover on Contract. Allowed values: 1, 2 |
| `hist_3_kw` | `string` | `varchar` |  |  | Log Turnover on Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arch_3` | `integer` | `int` |  |  | Purchase Contract History Archiving Implemented. Allowed values: 1, 2 |
| `arch_3_kw` | `string` | `varchar` |  |  | Purchase Contract History Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arco_3` | `integer` | `int` |  |  | Purchase Contract Archiving Implemented. Allowed values: 1, 2 |
| `arco_3_kw` | `string` | `varchar` |  |  | Purchase Contract Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcac_3` | `integer` | `int` |  |  | Print Contract Acknowledgment. Allowed values: 1, 2 |
| `pcac_3_kw` | `string` | `varchar` |  |  | Print Contract Acknowledgment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cmod_3` | `integer` | `int` |  |  | Reprint Contract Acknowledgement after Modification. Allowed values: 1, 2 |
| `cmod_3_kw` | `string` | `varchar` |  |  | Reprint Contract Acknowledgement after Modification (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `copy_3` | `integer` | `int` |  |  | Number of Extra Contract Acknowledgment Copies |
| `rpcs_3` | `integer` | `int` |  |  | Link with PCS Module. Allowed values: 1, 2 |
| `rpcs_3_kw` | `string` | `varchar` |  |  | Link with PCS Module (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imae_3` | `integer` | `int` |  |  | Action on Exceeding Maximum Quantity. Allowed values: 1, 2, 3 |
| `imae_3_kw` | `string` | `varchar` |  |  | Action on Exceeding Maximum Quantity (keyword). Allowed values: tdpur.imae.take.contract, tdpur.imae.ask.for.it, tdpur.imae.skip.contract |
| `muco_3` | `integer` | `int` |  |  | Interactive Contract Linking. Allowed values: 1, 2 |
| `muco_3_kw` | `string` | `varchar` |  |  | Interactive Contract Linking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sctp_3` | `integer` | `int` |  |  | Select Special Contract at Transfer from Planning. Allowed values: 1, 2 |
| `sctp_3_kw` | `string` | `varchar` |  |  | Select Special Contract at Transfer from Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `brla_3` | `integer` | `int` |  |  | Always Use Contract Price and Discount. Allowed values: 1, 2 |
| `brla_3_kw` | `string` | `varchar` |  |  | Always Use Contract Price and Discount (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `conf_3` | `integer` | `int` |  |  | Confirmation Needed?. Allowed values: 1, 2 |
| `conf_3_kw` | `string` | `varchar` |  |  | Confirmation Needed? (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sspc_3` | `integer` | `int` |  |  | Step Size for Purchase Contracts |
| `lfsc_3` | `integer` | `int` |  |  | Link Special Contract automatically. Allowed values: 1, 2 |
| `lfsc_3_kw` | `string` | `varchar` |  |  | Link Special Contract automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccgo_3` | `integer` | `int` |  |  | Consider Purchase Contracts when generating Purchase Orders. Allowed values: 1, 2 |
| `ccgo_3_kw` | `string` | `varchar` |  |  | Consider Purchase Contracts when generating Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vrck_3` | `integer` | `int` |  |  | Check Contract on Vendor Rating. Allowed values: 1, 2 |
| `vrck_3_kw` | `string` | `varchar` |  |  | Check Contract on Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vrmn_3` | `integer` | `int` |  |  | Warn if Vendor Rating is Below for Contract |
| `lpof_3` | `integer` | `int` |  |  | Log Financial Economic Transactions for Contracts. Allowed values: 1, 2 |
| `lpof_3_kw` | `string` | `varchar` |  |  | Log Financial Economic Transactions for Contracts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uscr_3` | `integer` | `int` |  |  | Change Requests for Purchase Contracts. Allowed values: 1, 2 |
| `uscr_3_kw` | `string` | `varchar` |  |  | Change Requests for Purchase Contracts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oscr_3` | `string` | `varchar` |  |  | Series for Purchase Contract Change Requests. Max length: 8 |
| `apcr_3` | `integer` | `int` |  |  | Process Purchase Contract Change Requests Automatically. Allowed values: 1, 2 |
| `apcr_3_kw` | `string` | `varchar` |  |  | Process Purchase Contract Change Requests Automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sedi_3` | `string` | `varchar` |  |  | EDI Release Series. Max length: 8 |
| `grpi_3` | `integer` | `int` |  |  | Generate Release per Item. Allowed values: 0, 1, 2 |
| `grpi_3_kw` | `string` | `varchar` |  |  | Generate Release per Item (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `ssps_3` | `integer` | `int` |  |  | Step Size for Purchase Schedule Lines |
| `segs_3` | `string` | `varchar` |  |  | Segment Set for Shipping Schedule. Max length: 3 |
| `ssmr_3` | `string` | `varchar` |  |  | Segment Set for Material Release. Max length: 3 |
| `ptss_3` | `string` | `varchar` |  |  | Issue Pattern for Shipping Schedule. Max length: 6 |
| `pttr_3` | `string` | `varchar` |  |  | Issue Pattern for Material Release. Max length: 6 |
| `uoia_3` | `integer` | `int` |  |  | Update Planned Inventory Transactions. Allowed values: 1, 2 |
| `uoia_3_kw` | `string` | `varchar` |  |  | Update Planned Inventory Transactions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dftm_3` | `string` | `varchar` |  |  | Default Template for Terms and Conditions. Max length: 9 |
| `crt1_3_1` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_1` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_2` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_2` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_3` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_3` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_4` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_4` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_5` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_5` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_6` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_6` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_7` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_7` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt1_3_8` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt1_3_kw_8` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Orders (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crt2_3_1` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_1` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_2` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_2` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_3` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_3` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_4` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_4` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_5` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_5` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_6` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_6` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_7` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_7` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt2_3_8` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders. Allowed values: 1, 2, 3 |
| `crt2_3_kw_8` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 2 for Purchase Orders (keyword). Allowed values: tdbpsear.crit2.pur.contracts, tdbpsear.crit2.item.bp, tdbpsear.crit2.not.applicable |
| `crt3_3_1` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_1` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_2` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_2` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_3` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_3` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_4` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_4` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_5` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_5` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_6` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_6` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_7` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_7` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crt3_3_8` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders. Allowed values: 1, 2, 3, 4, 5 |
| `crt3_3_kw_8` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Orders (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `sflo_3` | `integer` | `int` |  |  | Search All Defined Levels for Orders. Allowed values: 1, 2 |
| `sflo_3_kw` | `string` | `varchar` |  |  | Search All Defined Levels for Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crr1_3_1` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules. Allowed values: 1, 2, 3 |
| `crr1_3_kw_1` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crr1_3_2` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules. Allowed values: 1, 2, 3 |
| `crr1_3_kw_2` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crr1_3_3` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules. Allowed values: 1, 2, 3 |
| `crr1_3_kw_3` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crr1_3_4` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules. Allowed values: 1, 2, 3 |
| `crr1_3_kw_4` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 1 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit1.single.source, tdbpsear.crit1.preferred, tdbpsear.crit1.not.applicable |
| `crr3_3_1` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules. Allowed values: 1, 2, 3, 4, 5 |
| `crr3_3_kw_1` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crr3_3_2` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules. Allowed values: 1, 2, 3, 4, 5 |
| `crr3_3_kw_2` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crr3_3_3` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules. Allowed values: 1, 2, 3, 4, 5 |
| `crr3_3_kw_3` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `crr3_3_4` | `integer` | `int` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules. Allowed values: 1, 2, 3, 4, 5 |
| `crr3_3_kw_4` | `string` | `varchar` |  |  | Approved Business Partner Search Criteria 3 for Purchase Schedules (keyword). Allowed values: tdbpsear.crit3.central.contr, tdbpsear.crit3.decentral.contr, tdbpsear.crit3.item, tdbpsear.crit3.item.group, tdbpsear.crit3.not.applicable |
| `sadl_3` | `integer` | `int` |  |  | Search All Defined Levels for Schedules. Allowed values: 1, 2 |
| `sadl_3_kw` | `string` | `varchar` |  |  | Search All Defined Levels for Schedules (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `azpr_3` | `integer` | `int` |  |  | Allow Zero Contract Price. Allowed values: 1, 2 |
| `azpr_3_kw` | `string` | `varchar` |  |  | Allow Zero Contract Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psmn_3` | `integer` | `int` |  |  | Price Stage Mandatory for Contracts. Allowed values: 1, 2 |
| `psmn_3_kw` | `string` | `varchar` |  |  | Price Stage Mandatory for Contracts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hiss_3` | `integer` | `int` |  |  | Log Schedule History. Allowed values: 1, 2 |
| `hiss_3_kw` | `string` | `varchar` |  |  | Log Schedule History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ahis_3` | `integer` | `int` |  |  | Log Actual Schedule Receipt History. Allowed values: 1, 2 |
| `ahis_3_kw` | `string` | `varchar` |  |  | Log Actual Schedule Receipt History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `litk_3` | `integer` | `int` |  |  | Level of Schedule Intake History Logging. Allowed values: 10, 20 |
| `litk_3_kw` | `string` | `varchar` |  |  | Level of Schedule Intake History Logging (keyword). Allowed values: tdgen.litk.all, tdgen.litk.last |
| `arpr_3` | `integer` | `int` |  |  | Purchase Release Archiving Implemented. Allowed values: 1, 2 |
| `arpr_3_kw` | `string` | `varchar` |  |  | Purchase Release Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arps_3` | `integer` | `int` |  |  | Purchase Schedule Archiving Implemented. Allowed values: 1, 2 |
| `arps_3_kw` | `string` | `varchar` |  |  | Purchase Schedule Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cumm` | `integer` | `int` |  |  | CUM Model used. Allowed values: 1, 2, 12 |
| `cumm_kw` | `string` | `varchar` |  |  | CUM Model used (keyword). Allowed values: tdgen.cumm.order, tdgen.cumm.receipt, tdgen.cumm.not.applicable |
| `pagr_3` | `integer` | `int` |  |  | Process ASN During Goods Receipt. Allowed values: 1, 2 |
| `pagr_3_kw` | `string` | `varchar` |  |  | Process ASN During Goods Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nauc_3` | `integer` | `int` |  |  | Update Shipped CUM for Receipts without ASN. Allowed values: 1, 2 |
| `nauc_3_kw` | `string` | `varchar` |  |  | Update Shipped CUM for Receipts without ASN (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tprl` | `integer` | `int` |  |  | Tax Level Release Line. Allowed values: 1, 2 |
| `tprl_kw` | `string` | `varchar` |  |  | Tax Level Release Line (keyword). Allowed values: tdpur.tprl.detail, tdpur.tprl.line |
| `cboa_4` | `integer` | `int` |  |  | Confirm Back Orders automatically. Allowed values: 1, 2 |
| `cboa_4_kw` | `string` | `varchar` |  |  | Confirm Back Orders automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `potd_4` | `integer` | `int` |  |  | Goods Amount/Discount on Purchase Orders. Allowed values: 1, 2, 3 |
| `potd_4_kw` | `string` | `varchar` |  |  | Goods Amount/Discount on Purchase Orders (keyword). Allowed values: tdpur.pram.nett.no.disc, tdpur.pram.gross.and.disc, tdpur.pram.nett.and.disc |
| `csoc_4` | `integer` | `int` |  |  | Cost/Service Items on Claim Notes. Allowed values: 1, 2, 3, 4 |
| `csoc_4_kw` | `string` | `varchar` |  |  | Cost/Service Items on Claim Notes (keyword). Allowed values: tdsls.cson.no, tdsls.cson.only.cost, tdsls.cson.only.service, tdsls.cson.both |
| `prcp_4` | `integer` | `int` |  |  | Number of Extra Reminder Copies |
| `back_4` | `integer` | `int` |  |  | Backorders Allowed. Allowed values: 1, 2 |
| `back_4_kw` | `string` | `varchar` |  |  | Backorders Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngpo_4` | `string` | `varchar` |  |  | Number Group for Purchase Orders / Purchase Schedules. Max length: 3 |
| `ngpi_4` | `string` | `varchar` |  |  | Number Group for Purchase Invoice. Max length: 3 |
| `sepi_4` | `string` | `varchar` |  |  | Series for Purchase Invoice. Max length: 8 |
| `sspo_4` | `integer` | `int` |  |  | Step Size for Purchase Order Lines |
| `ngar_4` | `string` | `varchar` |  |  | Number Group for Approval Rules. Max length: 3 |
| `aarb` | `integer` | `int` |  |  | Basis for Approval Rules. Allowed values: 1, 2, 3 |
| `aarb_kw` | `string` | `varchar` |  |  | Basis for Approval Rules (keyword). Allowed values: tdaar.basis.direct, tdaar.basis.exceptions, tdaar.basis.none |
| `aprm_4` | `integer` | `int` |  |  | Approval Rules Mandatory. Allowed values: 1, 2 |
| `aprm_4_kw` | `string` | `varchar` |  |  | Approval Rules Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cspn_4` | `integer` | `int` |  |  | First Position Number for Additional Costs Sets |
| `pitd_4` | `integer` | `int` |  |  | Goods Amount/Discount on Purchase Invoices. Allowed values: 1, 2, 3 |
| `pitd_4_kw` | `string` | `varchar` |  |  | Goods Amount/Discount on Purchase Invoices (keyword). Allowed values: tdpur.pram.nett.no.disc, tdpur.pram.gross.and.disc, tdpur.pram.nett.and.disc |
| `arac_4` | `integer` | `int` |  |  | Automatic Recalculation of Additional Costs. Allowed values: 1, 2, 3 |
| `arac_4_kw` | `string` | `varchar` |  |  | Automatic Recalculation of Additional Costs (keyword). Allowed values: tdsls.acpd.no, tdsls.acpd.automatic, tdsls.acpd.interactive |
| `erac_4` | `integer` | `int` |  |  | Engineering Revisions Active in Purchase. Allowed values: 1, 2 |
| `erac_4_kw` | `string` | `varchar` |  |  | Engineering Revisions Active in Purchase (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `erdc_4` | `integer` | `int` |  |  | Engineering Revisions on Purchase Documents. Allowed values: 1, 2 |
| `erdc_4_kw` | `string` | `varchar` |  |  | Engineering Revisions on Purchase Documents (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mavp_4` | `integer` | `int` |  |  | Method of Calculating Average Purchase Price. Allowed values: 1, 2 |
| `mavp_4_kw` | `string` | `varchar` |  |  | Method of Calculating Average Purchase Price (keyword). Allowed values: tdpur.mavp.cum.purchase, tdpur.mavp.current.stock |
| `clro_4` | `integer` | `int` |  |  | Delete Order Data if Received Completely. Allowed values: 1, 2 |
| `clro_4_kw` | `string` | `varchar` |  |  | Delete Order Data if Received Completely (keyword). Allowed values: tdpur.clro.order, tdpur.clro.orderline |
| `mdoh_4` | `integer` | `int` |  |  | Method of Deleting Order History Data. Allowed values: 1, 2 |
| `mdoh_4_kw` | `string` | `varchar` |  |  | Method of Deleting Order History Data (keyword). Allowed values: tdpur.clro.order, tdpur.clro.orderline |
| `caiq_4` | `integer` | `int` |  |  | Check Order on Actual Quotations. Allowed values: 1, 2 |
| `caiq_4_kw` | `string` | `varchar` |  |  | Check Order on Actual Quotations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `otpd_4` | `string` | `varchar` |  |  | Purchase Order Type for Direct Deliveries. Max length: 3 |
| `otsd_4` | `string` | `varchar` |  |  | Order Type for Service Direct Deliveries. Max length: 3 |
| `otdr_4` | `string` | `varchar` |  |  | Return Order Type for Direct Deliveries. Max length: 3 |
| `otsr_4` | `string` | `varchar` |  |  | Return Order Type for Service Direct Deliveries. Max length: 3 |
| `ospd_4` | `string` | `varchar` |  |  | Purchase Order Series for Direct Deliveries. Max length: 8 |
| `ossd_4` | `string` | `varchar` |  |  | Order Series for Service Direct Deliveries. Max length: 8 |
| `cnsp_4` | `string` | `varchar` |  |  | Order Type for Consignment Payment. Max length: 3 |
| `comm_4_1` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_1` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_2` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_2` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_3` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_3` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_4` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_4` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_5` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_5` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_6` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_6` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_7` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_7` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_8` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_8` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_9` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_9` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_10` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_10` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_11` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_11` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_12` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_12` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_13` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_13` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_14` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_14` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_15` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_15` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_16` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_16` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_17` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_17` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_18` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_18` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_19` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_19` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `comm_4_20` | `integer` | `int` |  |  | Commingling for Origin. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29 |
| `comm_4_kw_20` | `string` | `varchar` |  |  | Commingling for Origin (keyword). Allowed values: , tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit |
| `vrck_4` | `integer` | `int` |  |  | Check Order on Vendor Rating. Allowed values: 1, 2 |
| `vrck_4_kw` | `string` | `varchar` |  |  | Check Order on Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vrmn_4` | `integer` | `int` |  |  | Warn if Vendor Rating is Below for Order |
| `crcd_4` | `string` | `varchar` |  |  | Change Reason for Backorders. Max length: 6 |
| `ctcd_4` | `string` | `varchar` |  |  | Change Type for Backorders. Max length: 6 |
| `cral_4` | `string` | `varchar` |  |  | Default Change Reason Code for Add Order Line. Max length: 6 |
| `crcl_4` | `string` | `varchar` |  |  | Default Change Reason Code for Cancel Order Line. Max length: 6 |
| `crml_4` | `string` | `varchar` |  |  | Default Change Reason Code for Change Order Line. Max length: 6 |
| `ctal_4` | `string` | `varchar` |  |  | Default Change Type for Add Order Line. Max length: 6 |
| `ctcl_4` | `string` | `varchar` |  |  | Default Change Type for Cancel Order Line. Max length: 6 |
| `ctml_4` | `string` | `varchar` |  |  | Default Change Type for Change Order Line. Max length: 6 |
| `sdtc_4` | `integer` | `int` |  |  | Search Date for Terms and Conditions. Allowed values: 1, 2, 3 |
| `sdtc_4_kw` | `string` | `varchar` |  |  | Search Date for Terms and Conditions (keyword). Allowed values: tdsdtp.order.date, tdsdtp.system.date, tdsdtp.delivery.date |
| `ccma_4` | `integer` | `int` |  |  | Change Codes Mandatory. Allowed values: 1, 2 |
| `ccma_4_kw` | `string` | `varchar` |  |  | Change Codes Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aacn_4` | `integer` | `int` |  |  | Automatic Assignment of Change Order Sequence Numbers. Allowed values: 1, 2 |
| `aacn_4_kw` | `string` | `varchar` |  |  | Automatic Assignment of Change Order Sequence Numbers (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdgo_4` | `integer` | `int` |  |  | Use Creation Date as Order Date when generating Purchase Order. Allowed values: 1, 2 |
| `cdgo_4_kw` | `string` | `varchar` |  |  | Use Creation Date as Order Date when generating Purchase Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcpo_4` | `integer` | `int` |  |  | Reopen Closed Purchase Orders Allowed. Allowed values: 1, 2 |
| `rcpo_4_kw` | `string` | `varchar` |  |  | Reopen Closed Purchase Orders Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `otsu_4` | `string` | `varchar` |  |  | Order Type for Subcontracting. Max length: 3 |
| `ossu_4` | `string` | `varchar` |  |  | Order Series for Subcontracting. Max length: 8 |
| `otss_4` | `string` | `varchar` |  |  | Order Type for Service Subcontracting. Max length: 3 |
| `osss_4` | `string` | `varchar` |  |  | Order Series for Service Subcontracting. Max length: 8 |
| `swip_4` | `integer` | `int` |  |  | Valuation for Subcontracting WIP. Allowed values: 1, 2 |
| `swip_4_kw` | `string` | `varchar` |  |  | Valuation for Subcontracting WIP (keyword). Allowed values: tcwip.val.standard.costs, tcwip.val.actual.costs |
| `otcf_4` | `string` | `varchar` |  |  | Order Type for Customer Furnished Materials. Max length: 3 |
| `oscf_4` | `string` | `varchar` |  |  | Order Series for Customer Furnished Materials. Max length: 8 |
| `otrr_4` | `string` | `varchar` |  |  | Order Type for Return Rejection. Max length: 3 |
| `osrr_4` | `string` | `varchar` |  |  | Order Series for Return Rejection. Max length: 8 |
| `pdbo_4` | `integer` | `int` |  |  | Price Determination Based on. Allowed values: 10, 20, 90 |
| `pdbo_4_kw` | `string` | `varchar` |  |  | Price Determination Based on (keyword). Allowed values: tcpdbo.replenishment, tcpdbo.consumption, tcpdbo.not.appl |
| `psmn_4` | `integer` | `int` |  |  | Price Stage Mandatory for Purchase Orders. Allowed values: 1, 2 |
| `psmn_4_kw` | `string` | `varchar` |  |  | Price Stage Mandatory for Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `craf_4` | `integer` | `int` |  |  | Confirm Receipt as Final. Allowed values: 1, 2 |
| `craf_4_kw` | `string` | `varchar` |  |  | Confirm Receipt as Final (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `obli_4` | `integer` | `int` |  |  | Order Blocking Implemented. Allowed values: 1, 2 |
| `obli_4_kw` | `string` | `varchar` |  |  | Order Blocking Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ichr_4` | `string` | `varchar` |  |  | Import Compliance Hold Reason. Max length: 3 |
| `lchr_4` | `string` | `varchar` |  |  | Letter of Credit Hold Reason. Max length: 3 |
| `uscr_4` | `integer` | `int` |  |  | Change Requests for Purchase Orders. Allowed values: 1, 2 |
| `uscr_4_kw` | `string` | `varchar` |  |  | Change Requests for Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oscr_4` | `string` | `varchar` |  |  | Series for Purchase Order Change Requests. Max length: 8 |
| `apcr_4` | `integer` | `int` |  |  | Process Purchase Order Change Requests Automatically. Allowed values: 1, 2 |
| `apcr_4_kw` | `string` | `varchar` |  |  | Process Purchase Order Change Requests Automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hisp_5` | `integer` | `int` |  |  | Log Order History. Allowed values: 1, 2 |
| `hisp_5_kw` | `string` | `varchar` |  |  | Log Order History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ahis_5` | `integer` | `int` |  |  | Log Actual Order Receipt History. Allowed values: 1, 2 |
| `ahis_5_kw` | `string` | `varchar` |  |  | Log Actual Order Receipt History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `litk_5` | `integer` | `int` |  |  | Level of Order Intake History Logging. Allowed values: 10, 20 |
| `litk_5_kw` | `string` | `varchar` |  |  | Level of Order Intake History Logging (keyword). Allowed values: tdgen.litk.all, tdgen.litk.last |
| `hism_5` | `integer` | `int` |  |  | Start Logging Order History at. Allowed values: 10, 20 |
| `hism_5_kw` | `string` | `varchar` |  |  | Start Logging Order History at (keyword). Allowed values: tdgen.hism.entry, tdgen.hism.approval |
| `arpo_4` | `integer` | `int` |  |  | Purchase Order Archiving Implemented. Allowed values: 1, 2 |
| `arpo_4_kw` | `string` | `varchar` |  |  | Purchase Order Archiving Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pccd_5` | `integer` | `int` |  |  | Mandatory Entry of Change Codes. Allowed values: 1, 2 |
| `pccd_5_kw` | `string` | `varchar` |  |  | Mandatory Entry of Change Codes (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ptax_5` | `integer` | `int` |  |  | Print Tax on External Purchase Documents. Allowed values: 1, 2 |
| `ptax_5_kw` | `string` | `varchar` |  |  | Print Tax on External Purchase Documents (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcsp_5` | `integer` | `int` |  |  | Base Price for Gross Profit Calculation. Allowed values: 1, 2 |
| `tcsp_5_kw` | `string` | `varchar` |  |  | Base Price for Gross Profit Calculation (keyword). Allowed values: tdpur.tcsp.copr, tdpur.tcsp.prip |
| `wdel_8` | `integer` | `int` |  |  | Delivery Time Weighting |
| `wdeq_8` | `integer` | `int` |  |  | Delivery Quantity Weighting |
| `wqly_8` | `integer` | `int` |  |  | Quality Weighting |
| `wcpr_8` | `integer` | `int` |  |  | Cost Performance Weighting |
| `wocf_8` | `integer` | `int` |  |  | Order Confirmation Weighting |
| `actp_8_1` | `float` | `float` |  |  | Actual Percentage |
| `actp_8_2` | `float` | `float` |  |  | Actual Percentage |
| `actp_8_3` | `float` | `float` |  |  | Actual Percentage |
| `actp_8_4` | `float` | `float` |  |  | Actual Percentage |
| `actp_8_5` | `float` | `float` |  |  | Actual Percentage |
| `dtmt_8` | `integer` | `int` |  |  | Delivery Time Table. Allowed values: 1, 2 |
| `dtmt_8_kw` | `string` | `varchar` |  |  | Delivery Time Table (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dqtt_8` | `integer` | `int` |  |  | Delivery Quantity Table. Allowed values: 1, 2 |
| `dqtt_8_kw` | `string` | `varchar` |  |  | Delivery Quantity Table (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dqlt_8` | `integer` | `int` |  |  | Delivery Quality Table. Allowed values: 1, 2 |
| `dqlt_8_kw` | `string` | `varchar` |  |  | Delivery Quality Table (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ocft_8` | `integer` | `int` |  |  | Order Confirmation Table. Allowed values: 1, 2 |
| `ocft_8_kw` | `string` | `varchar` |  |  | Order Confirmation Table (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprt_8` | `integer` | `int` |  |  | Cost Performance Table. Allowed values: 1, 2 |
| `cprt_8_kw` | `string` | `varchar` |  |  | Cost Performance Table (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wmth_8` | `integer` | `int` |  |  | Weighting Method. Allowed values: 1, 2, 3 |
| `wmth_8_kw` | `string` | `varchar` |  |  | Weighting Method (keyword). Allowed values: tdpur.wmth.8.turnover, tdpur.wmth.8.pieces, tdpur.wmth.8.poc |
| `cpdt_8` | `string` | `varchar` |  |  | Vendor Rating Period Table. Max length: 6 |
| `smth_8` | `integer` | `int` |  |  | Smoothing Method. Allowed values: 1, 2 |
| `smth_8_kw` | `string` | `varchar` |  |  | Smoothing Method (keyword). Allowed values: tdpur.smth.8.moveaverage, tdpur.smth.8.factor |
| `sfac_8` | `float` | `float` |  |  | Smoothing Factor |
| `wchd_8` | `integer` | `int` |  |  | Weightings Changed. Allowed values: 1, 2 |
| `wchd_8_kw` | `string` | `varchar` |  |  | Weightings Changed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smap_8` | `integer` | `int` |  |  | Moving Average Periods |
| `cnsr_4` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `spoa_4` | `string` | `varchar` |  |  | Obsolete. Max length: 8 |
| `cpdt_3` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `clca_2` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `clca_2_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `drdd_4` | `string` | `varchar` |  |  | Obsolete. Max length: 15 |
| `xapp_2` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `xapp_2_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `prcp_2` | `integer` | `int` |  |  | Obsolete |
| `puom_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `rbng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpi_1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `cset_1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur190 RFQ Criterion Sets |
| `ngpr_2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngco_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpc_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngsr_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `segs_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu013 Segment Sets |
| `ssmr_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu013 Segment Sets |
| `ptss_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp090 Patterns |
| `pttr_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp090 Patterns |
| `dftm_3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctrm100 Terms and Conditions |
| `ngpo_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpi_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngar_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `otpd_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otsd_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otdr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otsr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `cnsp_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `crcd_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `cral_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `crcl_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `crml_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctal_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `ctcl_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `ctml_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `otsu_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otss_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otcf_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `otrr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `ichr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs210 Hold Reasons |
| `lchr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs210 Hold Reasons |
| `cpdt_8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cnsr_4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
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
