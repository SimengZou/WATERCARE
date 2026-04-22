# tppin080

LN tppin080 Fees and Penalties table, Generated 2026-04-10T19:42:06Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin080` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `cfln` |
| **Column count** | 87 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Fee |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `ftyp` | `integer` | `int` |  |  | Fee Type. Allowed values: 10, 20, 30, 40 |
| `ftyp_kw` | `string` | `varchar` |  |  | Fee Type (keyword). Allowed values: tppin.ftyp.fixed.fee, tppin.ftyp.award.fee, tppin.ftyp.incentive.fee, tppin.ftyp.penalty |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `sprd` | `timestamp` | `timestamp_ntz` |  |  | Start Performance Period |
| `fprd` | `timestamp` | `timestamp_ntz` |  |  | Finish Performance Period |
| `cpro` | `string` | `varchar` |  |  | Revenue Code. Max length: 8 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | Triggering Activity. Max length: 16 |
| `sidt` | `timestamp` | `timestamp_ntz` |  |  | Planned Invoice Date |
| `ofcn` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `cinm` | `string` | `varchar` |  |  | Invoicing Method. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `famt` | `float` | `float` |  |  | Fee Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `pbbl` | `float` | `float` |  |  | Probability |
| `iamt` | `float` | `float` |  |  | Invoice Amount |
| `invo` | `integer` | `int` |  |  | Approved for Invoicing. Allowed values: 1, 2 |
| `invo_kw` | `string` | `varchar` |  |  | Approved for Invoicing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `idln` | `integer` | `int` |  |  | Invoice Document Line |
| `indt` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `toin` | `integer` | `int` |  |  | Use Text on Invoice. Allowed values: 1, 2 |
| `toin_kw` | `string` | `varchar` |  |  | Use Text on Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `text` | `integer` | `int` |  |  | Text |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `cinm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs055 Invoicing Methods |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `iamt_refc` | `float` | `float` |  |  | CC: Invoice Amount in Reference Currency |
| `iamt_cntc` | `float` | `float` |  |  | CC: Invoice Amount in Contract Currency |
| `iamt_prjc` | `float` | `float` |  |  | CC: Invoice Amount in Project Currency |
| `iamt_homc` | `float` | `float` |  |  | CC: Invoice Amount in Local Currency |
| `iamt_rpc1` | `float` | `float` |  |  | CC: Invoice Amount in Reporting Currency 1 |
| `iamt_rpc2` | `float` | `float` |  |  | CC: Invoice Amount in Reporting Currency 2 |
| `iamt_dwhc` | `float` | `float` |  |  | CC: Invoice Amount in Data Warehouse Currency |
| `famt_refc` | `float` | `float` |  |  | CC: Fee Amount in Reference Currency |
| `famt_cntc` | `float` | `float` |  |  | CC: Fee Amount in Contract Currency |
| `famt_prjc` | `float` | `float` |  |  | CC: Fee Amount in Project Currency |
| `famt_homc` | `float` | `float` |  |  | CC: Fee Amount in Local Currency |
| `famt_rpc1` | `float` | `float` |  |  | CC: Fee Amount in Reporting Currency 1 |
| `famt_rpc2` | `float` | `float` |  |  | CC: Fee Amount in Reporting Currency 2 |
| `famt_dwhc` | `float` | `float` |  |  | CC: Fee Amount in Data Warehouse Currency |
| `cprj_cpro` | `string` | `varchar` |  |  | CC: Project Revenue Code. Max length: 25 |
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
