# tccom122

LN tccom122 Invoice-from Business Partners table, Generated 2026-04-10T19:41:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom122` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ifbp`, `cofc` |
| **Column count** | 134 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ifbp` | `string` | `varchar` | `PK` | `not_null` | (required) Invoice-from Business Partner. Max length: 9 |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `bpst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 254 |
| `bpst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.bpst.active, tccom.bpst.inactive, tccom.bpst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `vryn` | `integer` | `int` |  |  | Vendor Rating. Allowed values: 1, 2 |
| `vryn_kw` | `string` | `varchar` |  |  | Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone Number. Max length: 32 |
| `tlcy` | `string` | `varchar` |  |  | Country for Phone Number. Max length: 6 |
| `tlci` | `string` | `varchar` |  |  | City/Area Code for Phone Number. Max length: 15 |
| `tlln` | `string` | `varchar` |  |  | Local Number for Phone Number. Max length: 32 |
| `tlex` | `string` | `varchar` |  |  | Extension for Phone Number. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax Number. Max length: 32 |
| `tfcy` | `string` | `varchar` |  |  | Country for Fax Number. Max length: 6 |
| `tfci` | `string` | `varchar` |  |  | City/Area Code for Fax Number. Max length: 15 |
| `tfln` | `string` | `varchar` |  |  | Local Number for Fax Number. Max length: 32 |
| `tfex` | `string` | `varchar` |  |  | Extension for Fax Number. Max length: 15 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cfsg` | `string` | `varchar` |  |  | Financial Supplier Group. Max length: 3 |
| `mstm` | `string` | `varchar` |  |  | Statement Method. Max length: 3 |
| `subc` | `integer` | `int` |  |  | Subcontracting. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bloc` | `string` | `varchar` |  |  | Hold Reason for Purchase Invoices. Max length: 3 |
| `refr__en_us` | `string` | `varchar` |  | `not_null` | (required) Transaction Reference - base language. Max length: 32 |
| `docm` | `string` | `varchar` |  |  | Document Method. Max length: 3 |
| `ncin` | `integer` | `int` |  |  | Number of Extra Invoice Copies |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payments. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spss` | `string` | `varchar` |  |  | Stage Payment Schedule. Max length: 3 |
| `sbmt` | `string` | `varchar` |  |  | Self-Billing Method. Max length: 3 |
| `usin` | `integer` | `int` |  |  | Include Number Group in Invoice Document. Allowed values: 1, 2 |
| `usin_kw` | `string` | `varchar` |  |  | Include Number Group in Invoice Document (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nrgr` | `string` | `varchar` |  |  | Number Group. Max length: 3 |
| `seri` | `string` | `varchar` |  |  | Series. Max length: 8 |
| `trsc` | `string` | `varchar` |  |  | Transaction Template. Max length: 3 |
| `leac` | `string` | `varchar` |  |  | Ledger Account. Max length: 12 |
| `crlr` | `float` | `float` |  |  | Credit Limit |
| `dlcr` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Credit Review |
| `pana` | `string` | `varchar` |  |  | Payables Analyst. Max length: 9 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `rpay` | `string` | `varchar` |  |  | Payment Terms for Credit Notes. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `rcrs` | `string` | `varchar` |  |  | Late Payment Surcharge for Credit Notes. Max length: 3 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `rpym` | `string` | `varchar` |  |  | Payment Method for Credit Notes. Max length: 3 |
| `paya` | `string` | `varchar` |  |  | Payment Agreement. Max length: 3 |
| `dim1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `etyp` | `integer` | `int` |  |  | Expense Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 |
| `etyp_kw` | `string` | `varchar` |  |  | Expense Type (keyword). Allowed values: tcetyp.not.applicable, tcetyp.fees, tcetyp.commissions, tcetyp.brokerage, tcetyp.drawbacks, tcetyp.directors.fees, tcetyp.perform.rights, tcetyp.inventor.rights, tcetyp.wages.salaries, tcetyp.food, tcetyp.housing, tcetyp.coach, tcetyp.other.benefits, tcetyp.flat.rate.benef, tcetyp.reimburse.just, tcetyp.meeting.costs, tcetyp.reduced.rate, tcetyp.waiver.deduct, tcetyp.car, tcetyp.tools.nict, tcetyp.other.advantage, tcetyp.fixed.alloc |
| `sirn` | `string` | `varchar` |  |  | SIREN Code. Max length: 9 |
| `ecod` | `string` | `varchar` |  |  | Establishment Code. Max length: 5 |
| `bpcl` | `string` | `varchar` |  |  | Business Partner Tax Classification. Max length: 12 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `inma` | `integer` | `int` |  |  | Invoice Number Mandatory. Allowed values: 1, 2, 3 |
| `inma_kw` | `string` | `varchar` |  |  | Invoice Number Mandatory (keyword). Allowed values: tcyndf.yes, tcyndf.no, tcyndf.default |
| `dinc` | `integer` | `int` |  |  | Duplicate Invoice Check Parameters. Allowed values: 1, 2 |
| `dinc_kw` | `string` | `varchar` |  |  | Duplicate Invoice Check Parameters (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dial` | `integer` | `int` |  |  | Duplicate Invoice Number Allowed. Allowed values: 1, 2 |
| `dial_kw` | `string` | `varchar` |  |  | Duplicate Invoice Number Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `incc` | `integer` | `int` |  |  | Apply Checks across Companies. Allowed values: 1, 2 |
| `incc_kw` | `string` | `varchar` |  |  | Apply Checks across Companies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `incb` | `integer` | `int` |  |  | Check Invoice Number against All Business Partners. Allowed values: 1, 2 |
| `incb_kw` | `string` | `varchar` |  |  | Check Invoice Number against All Business Partners (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `incy` | `integer` | `int` |  |  | Check Invoice Number within Year of Invoice. Allowed values: 1, 2 |
| `incy_kw` | `string` | `varchar` |  |  | Check Invoice Number within Year of Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdai` | `integer` | `int` |  |  | Check for Duplicate Amounts - Invoice Dates. Allowed values: 1, 2 |
| `cdai_kw` | `string` | `varchar` |  |  | Check for Duplicate Amounts - Invoice Dates (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdap` | `integer` | `int` |  |  | Check for Duplicate Amounts - Order Numbers. Allowed values: 1, 2 |
| `cdap_kw` | `string` | `varchar` |  |  | Check for Duplicate Amounts - Order Numbers (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eicd` | `integer` | `int` |  |  | Exclude Invoices with Correction Document. Allowed values: 1, 2 |
| `eicd_kw` | `string` | `varchar` |  |  | Exclude Invoices with Correction Document (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rrsi` | `integer` | `int` |  |  | Request to Reuse a Supplier Invoice Number. Allowed values: 1, 2 |
| `rrsi_kw` | `string` | `varchar` |  |  | Request to Reuse a Supplier Invoice Number (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcam` | `float` | `float` |  |  | Retention Capped Amount |
| `reas` | `string` | `varchar` |  |  | Reason for Payments. Max length: 3 |
| `exst` | `integer` | `int` |  |  | Exclude from Pay-to Business Partner Statistics. Allowed values: 1, 2 |
| `exst_kw` | `string` | `varchar` |  |  | Exclude from Pay-to Business Partner Statistics (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `spss_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs240 Installment Schedules Sets |
| `sbmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs057 Self-Billing Methods |
| `pana_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `rpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `rcrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `paya_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs206 Payment Agreements |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cfsg_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Financial Supplier Group |
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
