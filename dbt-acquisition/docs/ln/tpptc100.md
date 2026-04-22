# tpptc100

LN tpptc100 Elements table, Generated 2026-04-10T19:42:20Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cspa` |
| **Column count** | 106 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `txsp` | `integer` | `int` |  |  | Element for Headers. Allowed values: 1, 2 |
| `txsp_kw` | `string` | `varchar` |  |  | Element for Headers (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `stat` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2, 3 |
| `stat_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tpptc.stat.free, tpptc.stat.actual, tpptc.stat.definite |
| `ccst` | `integer` | `int` |  |  | Cost Control Status. Allowed values: 1, 2, 3, 4, 5 |
| `ccst_kw` | `string` | `varchar` |  |  | Cost Control Status (keyword). Allowed values: tpccst.free, tpccst.released, tpccst.onhold, tpccst.finished, tpccst.closed |
| `prps` | `integer` | `int` |  |  | Physical Progress. Allowed values: 1, 2 |
| `prps_kw` | `string` | `varchar` |  |  | Physical Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `prpf` | `integer` | `int` |  |  | Physical Progress Registration. Allowed values: 1, 2 |
| `prpf_kw` | `string` | `varchar` |  |  | Physical Progress Registration (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `lvps` | `integer` | `int` |  |  | Physical Progress Level. Allowed values: 1, 2 |
| `lvps_kw` | `string` | `varchar` |  |  | Physical Progress Level (keyword). Allowed values: tppdm.lvps.structpart, tppdm.lvps.structp.costctr |
| `cctr` | `integer` | `int` |  |  | Cost Control. Allowed values: 1, 2 |
| `cctr_kw` | `string` | `varchar` |  |  | Cost Control (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cctf` | `integer` | `int` |  |  | Cost Control Fixed. Allowed values: 1, 2 |
| `cctf_kw` | `string` | `varchar` |  |  | Cost Control Fixed (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Time |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Finish Time |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ltpr` | `timestamp` | `timestamp_ntz` |  |  | Trans.Date Price Result |
| `qust` | `integer` | `int` |  |  | Quantity in Budget Structure |
| `llnr` | `integer` | `int` |  |  | Low Level Number |
| `prin` | `integer` | `int` |  |  | Progress based Invoicing. Allowed values: 1, 2, 3, 4 |
| `prin_kw` | `string` | `varchar` |  |  | Progress based Invoicing (keyword). Allowed values: tppdm.prin.yes, tppdm.prin.no, tppdm.prin.project.stage, tppdm.prin.na |
| `cspt` | `integer` | `int` |  |  | Progress for Invoice. Allowed values: 1, 2, 3 |
| `cspt_kw` | `string` | `varchar` |  |  | Progress for Invoice (keyword). Allowed values: tpptc.cspt.direct, tpptc.cspt.indirect, tpptc.cspt.na |
| `gris` | `integer` | `int` |  |  | Group Indirect Work |
| `qanc` | `float` | `float` |  |  | Budget Quantity |
| `qans` | `float` | `float` |  |  | Physical Progress Quantity |
| `qutm` | `float` | `float` |  |  | Number of Time Units |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `prsp` | `float` | `float` |  |  | Production Rate |
| `pris` | `float` | `float` |  |  | Sales Price |
| `sacu` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `setl` | `integer` | `int` |  |  | To be Settled. Allowed values: 1, 2 |
| `setl_kw` | `string` | `varchar` |  |  | To be Settled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `prss` | `float` | `float` |  |  | Settlement Price |
| `secu` | `string` | `varchar` |  |  | Settlement Currency. Max length: 3 |
| `sort__en_us` | `string` | `varchar` |  | `not_null` | (required) Sort Argument - base language. Max length: 16 |
| `dwar` | `string` | `varchar` |  |  | Default Project Warehouse. Max length: 6 |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hbyn` | `integer` | `int` |  |  | Use Holdback. Allowed values: 1, 2 |
| `hbyn_kw` | `string` | `varchar` |  |  | Use Holdback (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rtsa_1` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rtsa_2` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rtsa_3` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rfsa_1` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rfsa_2` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rfsa_3` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `rtse_1` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rtse_2` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rtse_3` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rfse_1` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rfse_2` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rfse_3` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rdse` | `timestamp` | `timestamp_ntz` |  |  | Rate Date Settlement |
| `wast` | `integer` | `int` |  |  | Work Authorization Status. Allowed values: 1, 2, 3, 4, 5 |
| `wast_kw` | `string` | `varchar` |  |  | Work Authorization Status (keyword). Allowed values: tppdm.wast.free, tppdm.wast.onhold, tppdm.wast.released, tppdm.wast.finished, tppdm.wast.closed |
| `rcod` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `ohdt` | `timestamp` | `timestamp_ntz` |  |  | On Hold Date |
| `pwar` | `string` | `varchar` |  |  | Priority Supply Warehouse. Max length: 6 |
| `cape` | `integer` | `int` |  |  | Capital Element. Allowed values: 1, 2 |
| `cape_kw` | `string` | `varchar` |  |  | Capital Element (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `anbr` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `rfac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `freq` | `float` | `float` |  |  | Frequency |
| `afrt` | `integer` | `int` |  |  | Apply Frequency to. Allowed values: 1, 2, 3, 10 |
| `afrt_kw` | `string` | `varchar` |  |  | Apply Frequency to (keyword). Allowed values: tppdm.afrt.multiply, tppdm.afrt.create.act, tppdm.afrt.create.ord, tppdm.afrt.na |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `sacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `secu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `dwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `pwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
