# tppdm790

LN tppdm790 Archived Projects table, Generated 2026-04-10T19:42:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm790` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `comp`, `cprj` |
| **Column count** | 98 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `comp` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dscb__en_us` | `string` | `varchar` |  | `not_null` | (required) Description II - base language. Max length: 30 |
| `padr` | `string` | `varchar` |  |  | Address Code Project. Max length: 9 |
| `kopr` | `integer` | `int` |  |  | Hierarchy Type. Allowed values: 1, 2, 3 |
| `kopr_kw` | `string` | `varchar` |  |  | Hierarchy Type (keyword). Allowed values: tppdm.kopr.main, tppdm.kopr.sub, tppdm.kopr.single |
| `tbdg` | `integer` | `int` |  |  | Main Project Budgeting. Allowed values: 1, 2 |
| `tbdg_kw` | `string` | `varchar` |  |  | Main Project Budgeting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `mprj` | `string` | `varchar` |  |  | Main Project. Max length: 9 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `seab__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key II - base language. Max length: 16 |
| `agty` | `integer` | `int` |  |  | Contract Type. Allowed values: 1, 2, 20 |
| `agty_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tppdm.agty.contract, tppdm.agty.prime.cost, tppdm.agty.na |
| `coty` | `integer` | `int` |  |  | Contract Execution. Allowed values: 1, 2 |
| `coty_kw` | `string` | `varchar` |  |  | Contract Execution (keyword). Allowed values: tppdm.coty.contract, tppdm.coty.subcontract |
| `invm` | `integer` | `int` |  |  | Invoice Type. Allowed values: 1, 2, 3, 4, 5, 20 |
| `invm_kw` | `string` | `varchar` |  |  | Invoice Type (keyword). Allowed values: tppdm.invm.prime.cost, tppdm.invm.unit.rate, tppdm.invm.instalment, tppdm.invm.inst.spec, tppdm.invm.delivery.based, tppdm.invm.na |
| `ccot` | `string` | `varchar` |  |  | Group. Max length: 3 |
| `ccam` | `string` | `varchar` |  |  | Acquiring Method. Max length: 3 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `scun` | `integer` | `int` |  |  | Several Customers. Allowed values: 1, 2 |
| `scun_kw` | `string` | `varchar` |  |  | Several Customers (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `copr` | `float` | `float` |  |  | Contract Amount |
| `rfcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Reference - base language. Max length: 15 |
| `cfac` | `string` | `varchar` |  |  | Financing Method. Max length: 6 |
| `cmud` | `string` | `varchar` |  |  | Sufferance Tax. Max length: 3 |
| `ncmp` | `integer` | `int` |  |  | Receiving Company |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `psta` | `integer` | `int` |  |  | Project Archive Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `psta_kw` | `string` | `varchar` |  |  | Project Archive Status (keyword). Allowed values: tppdm.psta.free, tppdm.psta.construction, tppdm.psta.finished, tppdm.psta.closed, tppdm.psta.archived, tppdm.psta.removed |
| `psdt` | `timestamp` | `timestamp_ntz` |  |  | Project Status Date |
| `arch` | `integer` | `int` |  |  | Archived. Allowed values: 1, 2 |
| `arch_kw` | `string` | `varchar` |  |  | Archived (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ardt` | `timestamp` | `timestamp_ntz` |  |  | Archive Date |
| `acmp` | `integer` | `int` |  |  | Archive-to Company |
| `aloc` | `string` | `varchar` |  |  | Archive User. Max length: 16 |
| `ptcs` | `float` | `float` |  |  | Lead Time |
| `espt` | `float` | `float` |  |  | Estimated Lead Time |
| `rept` | `float` | `float` |  |  | Actual Project Time |
| `rers` | `float` | `float` |  |  | Actual Result |
| `kptm` | `integer` | `int` |  |  | Project Lead Time Type. Allowed values: 1, 2 |
| `kptm_kw` | `string` | `varchar` |  |  | Project Lead Time Type (keyword). Allowed values: tppdm.kptm.workable.days, tppdm.kptm.calendar.days |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Time |
| `fddt` | `timestamp` | `timestamp_ntz` |  |  | Substantial Completion Date |
| `cobs` | `string` | `varchar` |  |  | Organization Breakdown Structure. Max length: 9 |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Finish Time |
| `ccur` | `string` | `varchar` |  |  | Project Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `intp` | `integer` | `int` |  |  | Internal Project. Allowed values: 0, 1, 2 |
| `intp_kw` | `string` | `varchar` |  |  | Internal Project (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `capp` | `integer` | `int` |  |  | Capital Project. Allowed values: 0, 1, 2 |
| `capp_kw` | `string` | `varchar` |  |  | Capital Project (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `anbr` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `ipin` | `integer` | `int` |  |  | Invoicing via PIN. Allowed values: 0, 1, 2 |
| `ipin_kw` | `string` | `varchar` |  |  | Invoicing via PIN (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `txta` | `integer` | `int` |  |  | Archive Text |
| `txtn` | `integer` | `int` |  |  | Project Text |
| `tmpl` | `integer` | `int` |  |  | Template. Allowed values: 0, 1, 2 |
| `tmpl_kw` | `string` | `varchar` |  |  | Template (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `padr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm065 Project Groups |
| `ccam_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm063 Acquiring Methods |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm059 Financing Methods |
| `cmud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm057 Sufferance Tax |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `cobs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm095 User Defined Structures |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
