# tpctm110

LN tpctm110 Contract Lines table, Generated 2026-04-10T19:41:51Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpctm110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln` |
| **Column count** | 301 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
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
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `rfcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) First Reference - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Second Reference - base language. Max length: 30 |
| `pcnt` | `string` | `varchar` |  |  | Prime Contractor. Max length: 9 |
| `pcrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Prime Contract Reference - base language. Max length: 40 |
| `prrt` | `string` | `varchar` |  |  | DPAS. Max length: 9 |
| `coty` | `integer` | `int` |  |  | Contract Execution. Allowed values: 1, 2 |
| `coty_kw` | `string` | `varchar` |  |  | Contract Execution (keyword). Allowed values: tppdm.coty.contract, tppdm.coty.subcontract |
| `codt` | `timestamp` | `timestamp_ntz` |  |  | Contract Award Date |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `rddt` | `timestamp` | `timestamp_ntz` |  |  | Contract Delivery Date |
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
| `esam` | `float` | `float` |  |  | Expected Sales Amount |
| `pfob` | `integer` | `int` |  |  | Base for Performance Obligation. Allowed values: 5, 10, 15 |
| `pfob_kw` | `string` | `varchar` |  |  | Base for Performance Obligation (keyword). Allowed values: tpctm.pfob.contract.amount, tpctm.pfob.trans.price, tpctm.pfob.not.applicable |
| `trpr` | `float` | `float` |  |  | Transaction Price |
| `ceil` | `integer` | `int` |  |  | Ceiling. Allowed values: 1, 2 |
| `ceil_kw` | `string` | `varchar` |  |  | Ceiling (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clam` | `float` | `float` |  |  | Ceiling Amount |
| `ivam` | `float` | `float` |  |  | Approved for Invoicing |
| `itod` | `float` | `float` |  |  | Invoiced to Date |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `sadr` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `spit` | `integer` | `int` |  |  | Sales Price for Material. Allowed values: 5, 10, 15 |
| `spit_kw` | `string` | `varchar` |  |  | Sales Price for Material (keyword). Allowed values: tpctm.spcp.markup, tpctm.spcp.sales.price, tpctm.spcp.not.applicable |
| `pcit` | `float` | `float` |  |  | Markup Material |
| `spts` | `integer` | `int` |  |  | Sales Price for Labor. Allowed values: 5, 10, 15 |
| `spts_kw` | `string` | `varchar` |  |  | Sales Price for Labor (keyword). Allowed values: tpctm.spcp.markup, tpctm.spcp.sales.price, tpctm.spcp.not.applicable |
| `pcts` | `float` | `float` |  |  | Markup Labor |
| `speq` | `integer` | `int` |  |  | Sales Price for Equipment. Allowed values: 5, 10, 15 |
| `speq_kw` | `string` | `varchar` |  |  | Sales Price for Equipment (keyword). Allowed values: tpctm.spcp.markup, tpctm.spcp.sales.price, tpctm.spcp.not.applicable |
| `pceq` | `float` | `float` |  |  | Markup Equipment |
| `spsb` | `integer` | `int` |  |  | Sales Price for Subcontracting. Allowed values: 5, 10, 15 |
| `spsb_kw` | `string` | `varchar` |  |  | Sales Price for Subcontracting (keyword). Allowed values: tpctm.spcp.markup, tpctm.spcp.sales.price, tpctm.spcp.not.applicable |
| `pcsb` | `float` | `float` |  |  | Markup Subcontracting |
| `spic` | `integer` | `int` |  |  | Sales Price for Sundry Cost. Allowed values: 5, 10, 15 |
| `spic_kw` | `string` | `varchar` |  |  | Sales Price for Sundry Cost (keyword). Allowed values: tpctm.spcp.markup, tpctm.spcp.sales.price, tpctm.spcp.not.applicable |
| `pcic` | `float` | `float` |  |  | Markup Sundry Cost |
| `enip` | `integer` | `int` |  |  | Enforce Invoicing Period. Allowed values: 1, 2 |
| `enip_kw` | `string` | `varchar` |  |  | Enforce Invoicing Period (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acpn` | `integer` | `int` |  |  | Acceptance Point. Allowed values: 10, 20, 30, 40 |
| `acpn_kw` | `string` | `varchar` |  |  | Acceptance Point (keyword). Allowed values: tpctm.acpn.source, tpctm.acpn.destination, tpctm.acpn.source.dest, tpctm.acpn.not.appl |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
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
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Tax Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Tax Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ptpp` | `string` | `varchar` |  |  | Payment Terms Progress Payment. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `cban` | `string` | `varchar` |  |  | Bank Account Code. Max length: 3 |
| `ubnk` | `integer` | `int` |  |  | Use Bank Relation. Allowed values: 1, 2 |
| `ubnk_kw` | `string` | `varchar` |  |  | Use Bank Relation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bank` | `string` | `varchar` |  |  | Bank Relation. Max length: 3 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `ccam` | `string` | `varchar` |  |  | Acquiring Method. Max length: 3 |
| `cfac` | `string` | `varchar` |  |  | Financing Method. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `ccot` | `string` | `varchar` |  |  | Group. Max length: 3 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `hrsn` | `string` | `varchar` |  |  | Hold Reason. Max length: 6 |
| `crby` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modified on |
| `lcrs` | `string` | `varchar` |  |  | Last Change Reason. Max length: 6 |
| `ltti` | `date` | `date` |  |  | Last Transferred to Invoicing |
| `blcl` | `string` | `varchar` |  |  | Billing Cycle. Max length: 6 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `fdis` | `integer` | `int` |  |  | Funding Distribution. Allowed values: 5, 10, 15, 20 |
| `fdis_kw` | `string` | `varchar` |  |  | Funding Distribution (keyword). Allowed values: tpctm.fdis.none, tpctm.fdis.sequence, tpctm.fdis.percentage, tpctm.fdis.amount |
| `flmt` | `float` | `float` |  |  | Funded Amount |
| `mfea__en_us` | `string` | `varchar` |  | `not_null` | (required) External Address Code - base language. Max length: 12 |
| `mftp` | `integer` | `int` |  |  | Marked For Type. Allowed values: 0, 10, 20 |
| `mftp_kw` | `string` | `varchar` |  |  | Marked For Type (keyword). Allowed values: , tpctm.mftp.address, tpctm.mftp.text |
| `mfad` | `string` | `varchar` |  |  | Marked For Address Code. Max length: 9 |
| `bver` | `string` | `varchar` |  |  | Bid Version. Max length: 3 |
| `bnum` | `string` | `varchar` |  |  | Bid Number. Max length: 8 |
| `esin` | `integer` | `int` |  |  | Extended Service Integration. Allowed values: 1, 2 |
| `esin_kw` | `string` | `varchar` |  |  | Extended Service Integration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `srvo` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `blck` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blck_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `swap` | `integer` | `int` |  |  | Scope of Work Applicable. Allowed values: 1, 2 |
| `swap_kw` | `string` | `varchar` |  |  | Scope of Work Applicable (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `osla` | `integer` | `int` |  |  | One Scope Line Per Activity. Allowed values: 1, 2 |
| `osla_kw` | `string` | `varchar` |  |  | One Scope Line Per Activity (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `afpa` | `integer` | `int` |  |  | Application for Payment Applicable. Allowed values: 1, 2 |
| `afpa_kw` | `string` | `varchar` |  |  | Application for Payment Applicable (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `gapp` | `integer` | `int` |  |  | Generate AFP from Physical Progress. Allowed values: 1, 2 |
| `gapp_kw` | `string` | `varchar` |  |  | Generate AFP from Physical Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `laft` | `integer` | `int` |  |  | Log AFP Financial Transactions. Allowed values: 1, 2 |
| `laft_kw` | `string` | `varchar` |  |  | Log AFP Financial Transactions (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `maas` | `integer` | `int` |  |  | Materials at Site. Allowed values: 1, 2 |
| `maas_kw` | `string` | `varchar` |  |  | Materials at Site (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `acar` | `integer` | `int` |  |  | Architect/Consultant Approval Required. Allowed values: 1, 2 |
| `acar_kw` | `string` | `varchar` |  |  | Architect/Consultant Approval Required (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `orna__en_us` | `string` | `varchar` |  | `not_null` | (required) Organization Name - base language. Max length: 35 |
| `arcn` | `string` | `varchar` |  |  | Architect Contact. Max length: 9 |
| `chrq` | `integer` | `int` |  |  | Change Request Applicable. Allowed values: 1, 2 |
| `chrq_kw` | `string` | `varchar` |  |  | Change Request Applicable (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `iscc` | `integer` | `int` |  |  | Is Contract Change Order. Allowed values: 1, 2 |
| `iscc_kw` | `string` | `varchar` |  |  | Is Contract Change Order (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ocon` | `string` | `varchar` |  |  | Original Contract. Max length: 9 |
| `ocln` | `string` | `varchar` |  |  | Original Contract Line. Max length: 8 |
| `tcoa` | `float` | `float` |  |  | Total Change Order Amount |
| `ecam` | `float` | `float` |  |  | Estimated Change Amount |
| `ecsm` | `float` | `float` |  |  | Estimated Cost Amount |
| `mftx` | `integer` | `int` |  |  | Marked For Text |
| `text` | `integer` | `int` |  |  | Text |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cmng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `pcnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `prrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs072 DPAS |
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
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `dfis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs240 Installment Schedules Sets |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
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
| `mfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `srvo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `arcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `mftx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `copr_refc` | `float` | `float` |  |  | CC: Contract Line Amount in Reference Currency |
| `copr_cntc` | `float` | `float` |  |  | CC: Contract Line Amount in Contract Currency |
| `copr_prjc` | `float` | `float` |  |  | CC: Contract Line Amount in Project Currency |
| `copr_homc` | `float` | `float` |  |  | CC: Contract Line Amount in Local currency |
| `copr_rpc1` | `float` | `float` |  |  | CC: Contract Line Amount in Reporting Currency 1 |
| `copr_rpc2` | `float` | `float` |  |  | CC: Contract Line Amount in Reporting Currency 2 |
| `copr_dwhc` | `float` | `float` |  |  | CC: Contract Line Amount in Data Warehouse Currency |
| `flmt_refc` | `float` | `float` |  |  | CC: Funded Amount in Reference Currency |
| `flmt_cntc` | `float` | `float` |  |  | CC: Funded Amount in Contract Currency |
| `flmt_prjc` | `float` | `float` |  |  | CC: Funded Amount in Project Currency |
| `flmt_homc` | `float` | `float` |  |  | CC: Funded Amount in Local Currency |
| `flmt_rpc1` | `float` | `float` |  |  | CC: Funded Amount in Reporting Currency 1 |
| `flmt_rpc2` | `float` | `float` |  |  | CC: Funded Amount in Reporting Currency 2 |
| `flmt_dwhc` | `float` | `float` |  |  | CC: Funded Amount in Data Warehouse Currency |
| `esam_refc` | `float` | `float` |  |  | CC: Expected Sales Amount in Reference Currency |
| `esam_cntc` | `float` | `float` |  |  | CC: Expected Sales Amount in Contract Currency |
| `esam_prjc` | `float` | `float` |  |  | CC: Expected Sales Amount in Project Currency |
| `esam_homc` | `float` | `float` |  |  | CC: Expected Sales Amount in Local Currency |
| `esam_rpc1` | `float` | `float` |  |  | CC: Expected Sales Amount in Reporting Currency 1 |
| `esam_rpc2` | `float` | `float` |  |  | CC: Expected Sales Amount in Reporting Currency 2 |
| `esam_dwhc` | `float` | `float` |  |  | CC: Expected Sales Amount in Data Warehouse Currency |
| `clam_refc` | `float` | `float` |  |  | CC: Ceiling Amount in Reference Currency |
| `clam_cntc` | `float` | `float` |  |  | CC: Ceiling Amount in Contract Currency |
| `clam_prjc` | `float` | `float` |  |  | CC: Ceiling Amount in Project Currency |
| `clam_homc` | `float` | `float` |  |  | CC: Ceiling Amount in Local Currency |
| `clam_rpc1` | `float` | `float` |  |  | CC: Ceiling Amount in Reporting Currency 1 |
| `clam_rpc2` | `float` | `float` |  |  | CC: Ceiling Amount in Reporting Currency 2 |
| `clam_dwhc` | `float` | `float` |  |  | CC: Ceiling Amount in Data Warehouse Currency |
| `itod_refc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reference Currency |
| `itod_cntc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Contract Currency |
| `itod_prjc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Project Currency |
| `itod_homc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Local Currency |
| `itod_rpc1` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reporting Currency 1 |
| `itod_rpc2` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Reporting Currency 2 |
| `itod_dwhc` | `float` | `float` |  |  | CC: Invoiced to Date Amount in Data Warehouse Currency |
| `cnln_fcmp` | `integer` | `int` |  |  | CC: Contract Line Financial Company |
| `next_bldt` | `string` | `varchar` |  |  | CC: Next Invoice Date. Max length: 30 |
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
