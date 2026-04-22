# tpctm100

LN tpctm100 Contracts table, Generated 2026-04-10T19:41:51Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpctm100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono` |
| **Column count** | 195 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `csts` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 40, 50 |
| `csts_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tpctm.csts.free, tpctm.csts.on.hold, tpctm.csts.active, tpctm.csts.closed, tpctm.csts.canceled |
| `prst` | `integer` | `int` |  |  | Previous Status. Allowed values: 10, 20, 30, 40, 50 |
| `prst_kw` | `string` | `varchar` |  |  | Previous Status (keyword). Allowed values: tpctm.csts.free, tpctm.csts.on.hold, tpctm.csts.active, tpctm.csts.closed, tpctm.csts.canceled |
| `ctyp` | `integer` | `int` |  |  | Contract Type. Allowed values: 10, 20, 30 |
| `ctyp_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tpctm.ctyp.fixed.price, tpctm.ctyp.cost.reimbrsmnt, tpctm.ctyp.time.materials |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `cmng` | `string` | `varchar` |  |  | Contract Manager. Max length: 9 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Second Reference - base language. Max length: 30 |
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) First Reference - base language. Max length: 30 |
| `coty` | `integer` | `int` |  |  | Contract Execution. Allowed values: 1, 2 |
| `coty_kw` | `string` | `varchar` |  |  | Contract Execution (keyword). Allowed values: tppdm.coty.contract, tppdm.coty.subcontract |
| `codt` | `timestamp` | `timestamp_ntz` |  |  | Contract Award Date |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `invm` | `integer` | `int` |  |  | Invoice Type. Allowed values: 1, 2, 3, 4, 5, 20 |
| `invm_kw` | `string` | `varchar` |  |  | Invoice Type (keyword). Allowed values: tppdm.invm.prime.cost, tppdm.invm.unit.rate, tppdm.invm.instalment, tppdm.invm.inst.spec, tppdm.invm.delivery.based, tppdm.invm.na |
| `cinm` | `string` | `varchar` |  |  | Invoicing Method. Max length: 3 |
| `ppim` | `string` | `varchar` |  |  | Progress Payment Invoicing Method. Max length: 3 |
| `disc` | `float` | `float` |  |  | Invoice Discount |
| `cidm` | `string` | `varchar` |  |  | Invoice Delivery Method. Max length: 3 |
| `styp` | `string` | `varchar` |  |  | Sales Type. Max length: 6 |
| `frvt` | `float` | `float` |  |  | Fee Revenue Threshold |
| `pgpm` | `integer` | `int` |  |  | Progress Payment. Allowed values: 1, 2 |
| `pgpm_kw` | `string` | `varchar` |  |  | Progress Payment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pppc` | `float` | `float` |  |  | Progress Payment % |
| `plpc` | `float` | `float` |  |  | Progress Liquidation % |
| `bcrp` | `integer` | `int` |  |  | Billable Cost Report. Allowed values: 1, 2 |
| `bcrp_kw` | `string` | `varchar` |  |  | Billable Cost Report (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `copr` | `float` | `float` |  |  | Contract Amount |
| `pcwa` | `float` | `float` |  |  | Labor Part of Contract Amount |
| `pcba` | `float` | `float` |  |  | Labor B-Account |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `ivam` | `float` | `float` |  |  | Approved for Invoicing |
| `itod` | `float` | `float` |  |  | Invoiced to Date |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `acpn` | `integer` | `int` |  |  | Acceptance Point. Allowed values: 10, 20, 30, 40 |
| `acpn_kw` | `string` | `varchar` |  |  | Acceptance Point (keyword). Allowed values: tpctm.acpn.source, tpctm.acpn.destination, tpctm.acpn.source.dest, tpctm.acpn.not.appl |
| `advn` | `integer` | `int` |  |  | Advance Payments. Allowed values: 10, 20, 30 |
| `advn_kw` | `string` | `varchar` |  |  | Advance Payments (keyword). Allowed values: tpctm.adpr.no, tpctm.adpr.by.contract, tpctm.adpr.by.contract.ln |
| `adtp` | `integer` | `int` |  |  | Advance Payment Type. Allowed values: 10, 20, 30 |
| `adtp_kw` | `string` | `varchar` |  |  | Advance Payment Type (keyword). Allowed values: tppin.adtp.request, tppin.adtp.invoice, tppin.adtp.not.applicable |
| `ulpd` | `integer` | `int` |  |  | Use Advance Liquidation Percentage. Allowed values: 5, 10 |
| `ulpd_kw` | `string` | `varchar` |  |  | Use Advance Liquidation Percentage (keyword). Allowed values: tpctm.adv.liq.full.liq, tpctm.adv.liq.liq.percentage |
| `lpad` | `float` | `float` |  |  | Liquidation % Advances |
| `tins` | `integer` | `int` |  |  | Installment Type. Allowed values: 10, 20, 30, 40 |
| `tins_kw` | `string` | `varchar` |  |  | Installment Type (keyword). Allowed values: tpctm.intp.amount, tpctm.intp.percentage, tpctm.intp.points, tpctm.intp.not.applicable |
| `poin` | `integer` | `int` |  |  | Number of Points |
| `lpin` | `float` | `float` |  |  | Liquidation % Installments |
| `dfis` | `string` | `varchar` |  |  | Default Installment Schedule. Max length: 3 |
| `prtx` | `integer` | `int` |  |  | Project Text on Invoice. Allowed values: 1, 2 |
| `prtx_kw` | `string` | `varchar` |  |  | Project Text on Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hldb` | `integer` | `int` |  |  | Holdback. Allowed values: 10, 20, 30 |
| `hldb_kw` | `string` | `varchar` |  |  | Holdback (keyword). Allowed values: tpctm.hldb.all.invoices, tpctm.hldb.based.on.progrs, tpctm.hldb.no.holdback |
| `hbpc` | `float` | `float` |  |  | Holdback Percentage |
| `ptsh` | `float` | `float` |  |  | Progress Threshold |
| `prgm` | `string` | `varchar` |  |  | Program. Max length: 30 |
| `pmng` | `string` | `varchar` |  |  | Program Manager. Max length: 9 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ptpp` | `string` | `varchar` |  |  | Payment Terms Progress Payment. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cban` | `string` | `varchar` |  |  | Bank Account Code. Max length: 3 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `ccam` | `string` | `varchar` |  |  | Acquiring Method. Max length: 3 |
| `cfac` | `string` | `varchar` |  |  | Financing Method. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `ccot` | `string` | `varchar` |  |  | Group. Max length: 3 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `hrsn` | `string` | `varchar` |  |  | Hold Reason. Max length: 6 |
| `ltti` | `date` | `date` |  |  | Last Transferred to Invoicing |
| `crby` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modified on |
| `lcrs` | `string` | `varchar` |  |  | Last Change Reason. Max length: 6 |
| `blcl` | `string` | `varchar` |  |  | Billing Cycle. Max length: 6 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `fdis` | `integer` | `int` |  |  | Funding Distribution. Allowed values: 0, 5, 10, 15, 20 |
| `fdis_kw` | `string` | `varchar` |  |  | Funding Distribution (keyword). Allowed values: , tpctm.fdis.none, tpctm.fdis.sequence, tpctm.fdis.percentage, tpctm.fdis.amount |
| `bver` | `string` | `varchar` |  |  | Bid Version. Max length: 3 |
| `bnum` | `string` | `varchar` |  |  | Bid Number. Max length: 8 |
| `guid` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `enip` | `integer` | `int` |  |  | Enforce Invoicing Period. Allowed values: 1, 2 |
| `enip_kw` | `string` | `varchar` |  |  | Enforce Invoicing Period (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dlip` | `float` | `float` |  |  | Defect Liability Period |
| `dliu` | `integer` | `int` |  |  | Defect Liability Unit. Allowed values: 5, 10, 15, 255 |
| `dliu_kw` | `string` | `varchar` |  |  | Defect Liability Unit (keyword). Allowed values: tppdm.tunit.days, tppdm.tunit.weeks, tppdm.tunit.months, tppdm.tunit.not.appl |
| `rhoa` | `integer` | `int` |  |  | Release Holdback After. Allowed values: 5, 10, 30 |
| `rhoa_kw` | `string` | `varchar` |  |  | Release Holdback After (keyword). Allowed values: tppdm.hore.final.afp, tppdm.hore.prj.compl, tppdm.hore.not.appl |
| `ldcr` | `float` | `float` |  |  | Liquidation Damages Clause/Rate |
| `ldcu` | `integer` | `int` |  |  | Liquidation Damages Clause Unit. Allowed values: 5, 10, 15, 255 |
| `ldcu_kw` | `string` | `varchar` |  |  | Liquidation Damages Clause Unit (keyword). Allowed values: tppdm.tunit.days, tppdm.tunit.weeks, tppdm.tunit.months, tppdm.tunit.not.appl |
| `acar` | `integer` | `int` |  |  | Architect/Consultant Approval Required. Allowed values: 1, 2 |
| `acar_kw` | `string` | `varchar` |  |  | Architect/Consultant Approval Required (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `orna__en_us` | `string` | `varchar` |  | `not_null` | (required) Organization Name - base language. Max length: 35 |
| `arcn` | `string` | `varchar` |  |  | Architect Contact. Max length: 9 |
| `chrq` | `integer` | `int` |  |  | Change Request Applicable. Allowed values: 1, 2 |
| `chrq_kw` | `string` | `varchar` |  |  | Change Request Applicable (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `iscc` | `integer` | `int` |  |  | Is Contract Change Order. Allowed values: 1, 2 |
| `iscc_kw` | `string` | `varchar` |  |  | Is Contract Change Order (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ocon` | `string` | `varchar` |  |  | Original Contract. Max length: 9 |
| `text` | `integer` | `int` |  |  | Text |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cmng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cinm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs055 Invoicing Methods |
| `ppim_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs055 Invoicing Methods |
| `cidm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs056 Invoice Delivery Methods |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs202 Sales Types |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cprj_bver_bnum_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpest300 Bid Headers |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `dfis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs240 Installment Schedules Sets |
| `prgm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm010 Programs |
| `pmng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ptpp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `ccam_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm063 Acquiring Methods |
| `cfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm059 Financing Methods |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `ccot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm065 Project Groups |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `hrsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `lcrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `blcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm030 Billing Cycles |
| `arcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `copr_refc` | `float` | `float` |  |  | CC: Contract Amount in Reference Currency |
| `copr_homc` | `float` | `float` |  |  | CC: Contract Amount in Local Currency |
| `copr_rpc1` | `float` | `float` |  |  | CC: Contract Amount in Reporting Currency 1 |
| `copr_rpc2` | `float` | `float` |  |  | CC: Contract Amount in Reporting Currency 2 |
| `copr_dwhc` | `float` | `float` |  |  | CC: Contract Amount in Data Warehouse Currency |
| `ivam_refc` | `float` | `float` |  |  | CC: Approved Invoice Amount in Reference Currency |
| `ivam_homc` | `float` | `float` |  |  | CC: Approved Invoice Amount in Local Currency |
| `ivam_rpc1` | `float` | `float` |  |  | CC: Approved Invoice Amount in Reporting Currency 1 |
| `ivam_rpc2` | `float` | `float` |  |  | CC: Approved Invoice Amount in Reporting Currency 2 |
| `ivam_dwhc` | `float` | `float` |  |  | CC: Approved Invoice Amount in Data Warehouse Currency |
| `itod_refc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reference Currency |
| `itod_homc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Local Currency |
| `itod_rpc1` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reporting Currency 1 |
| `itod_rpc2` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reporting Currency 2 |
| `itod_dwhc` | `float` | `float` |  |  | CC: Approved Invoice Amount in Data Warehouse Currency |
| `cono_fcmp` | `integer` | `int` |  |  | CC: Contract Financial Company |
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
