# tpptc311

LN tpptc311 Project Budget (IMS) table, Generated 2026-04-10T19:42:25Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc311` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ccal`, `cptc`, `cprj`, `sern`, `year`, `peri` |
| **Column count** | 146 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ccal` | `string` | `varchar` | `PK` | `not_null` | (required) Budget Cost Analysis. Max length: 3 |
| `cptc` | `string` | `varchar` | `PK` | `not_null` | (required) Code Period Table for Cost Control Period. Max length: 6 |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year of Actual Cost Control |
| `peri` | `integer` | `int` | `PK` | `not_null` | (required) Period for Actual Cost Control |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cotp` | `integer` | `int` |  |  | Cost Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `cotp_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tppdm.cotp.tasks, tppdm.cotp.materials, tppdm.cotp.equipment, tppdm.cotp.subcontracting, tppdm.cotp.indirect, tppdm.cotp.overhead |
| `refr` | `string` | `varchar` |  |  | Original Budget Line Reference. Max length: 50 |
| `item` | `string` | `varchar` |  |  | Cost Object. Max length: 47 |
| `task` | `string` | `varchar` |  |  | Labor. Max length: 8 |
| `cico` | `string` | `varchar` |  |  | Sundry Cost Code. Max length: 8 |
| `quan` | `float` | `float` |  |  | Net Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `qutm` | `float` | `float` |  |  | Amount of Time |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `norm` | `float` | `float` |  |  | Norm |
| `unrt` | `string` | `varchar` |  |  | Cost Rate Unit. Max length: 3 |
| `scpf` | `float` | `float` |  |  | Scrap Factor |
| `stat` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2, 3 |
| `stat_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tpptc.stat.free, tpptc.stat.actual, tpptc.stat.definite |
| `rsta` | `timestamp` | `timestamp_ntz` |  |  | Related Start Date |
| `rfin` | `timestamp` | `timestamp_ntz` |  |  | Related Finish Date |
| `btdt` | `timestamp` | `timestamp_ntz` |  |  | Budget Date |
| `cocu` | `string` | `varchar` |  |  | Cost Currency. Max length: 3 |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `aaoc_1` | `float` | `float` |  |  | Cost Amount in Home Currency |
| `aaoc_2` | `float` | `float` |  |  | Cost Amount in Home Currency |
| `aaoc_3` | `float` | `float` |  |  | Cost Amount in Home Currency |
| `aaoc_4` | `float` | `float` |  |  | Cost Amount in Home Currency |
| `lcta` | `float` | `float` |  |  | Landed Cost Amount |
| `lcaa_1` | `float` | `float` |  |  | Landed Cost Amount in Home Currency |
| `lcaa_2` | `float` | `float` |  |  | Landed Cost Amount in Home Currency |
| `lcaa_3` | `float` | `float` |  |  | Landed Cost Amount in Home Currency |
| `lcaa_4` | `float` | `float` |  |  | Landed Cost Amount in Home Currency |
| `clas` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `cona` | `float` | `float` |  |  | Contingency Amount |
| `cohc_1` | `float` | `float` |  |  | Contingency Amount in Home Currency |
| `cohc_2` | `float` | `float` |  |  | Contingency Amount in Home Currency |
| `cohc_3` | `float` | `float` |  |  | Contingency Amount in Home Currency |
| `cohc_4` | `float` | `float` |  |  | Contingency Amount in Home Currency |
| `prca_1` | `float` | `float` |  |  | Unit Cost |
| `prca_2` | `float` | `float` |  |  | Unit Cost |
| `prca_3` | `float` | `float` |  |  | Unit Cost |
| `prca_4` | `float` | `float` |  |  | Unit Cost |
| `pric` | `float` | `float` |  |  | Unit Cost in Cost Currency |
| `ratc` | `float` | `float` |  |  | Cost Rate in Cost Currency |
| `rtca_1` | `float` | `float` |  |  | Cost Rate |
| `rtca_2` | `float` | `float` |  |  | Cost Rate |
| `rtca_3` | `float` | `float` |  |  | Cost Rate |
| `rtca_4` | `float` | `float` |  |  | Cost Rate |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `sacu` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `amos` | `float` | `float` |  |  | Sales Amount in Sales Currency |
| `aaos_1` | `float` | `float` |  |  | Sales Amount |
| `aaos_2` | `float` | `float` |  |  | Sales Amount |
| `aaos_3` | `float` | `float` |  |  | Sales Amount |
| `aaos_4` | `float` | `float` |  |  | Sales Amount |
| `pris` | `float` | `float` |  |  | Sales Price in Sales Currency |
| `prsa_1` | `float` | `float` |  |  | Sales Price |
| `prsa_2` | `float` | `float` |  |  | Sales Price |
| `prsa_3` | `float` | `float` |  |  | Sales Price |
| `prsa_4` | `float` | `float` |  |  | Sales Price |
| `rats` | `float` | `float` |  |  | Sales Rate in Sales Currency |
| `rtsa_1` | `float` | `float` |  |  | Sales Rate |
| `rtsa_2` | `float` | `float` |  |  | Sales Rate |
| `rtsa_3` | `float` | `float` |  |  | Sales Rate |
| `rtsa_4` | `float` | `float` |  |  | Sales Rate |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `exeq` | `integer` | `int` |  |  | External Equipment. Allowed values: 1, 2, 3 |
| `exeq_kw` | `string` | `varchar` |  |  | External Equipment (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `adjm` | `integer` | `int` |  |  | Adjustment Indicator. Allowed values: 1, 2 |
| `adjm_kw` | `string` | `varchar` |  |  | Adjustment Indicator (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `dura` | `float` | `float` |  |  | Duration |
| `tmud` | `string` | `varchar` |  |  | Time Unit Duration. Max length: 3 |
| `basn` | `integer` | `int` |  |  | Baseline |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cprj_ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc300 Budget Cost Analysis Versions by Project |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_basn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss020 Baselines |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `unrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cocu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `clas_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `sacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `tmud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `pric_prjc` | `float` | `float` |  |  | CC: Cost Price in Project Currency |
| `amoc_prjc` | `float` | `float` |  |  | CC: Cost Amount in Project Currency |
| `lcos_prjc` | `float` | `float` |  |  | CC: Landed Cost Amount in Project Currency |
| `pris_prjc` | `float` | `float` |  |  | CC: Sales Price in Project Currency |
| `amos_prjc` | `float` | `float` |  |  | CC: Sales Amount in Project Currency |
| `cona_prjc` | `float` | `float` |  |  | CC: Contingency Amount in Project Currency |
| `pric_refc` | `float` | `float` |  |  | CC: Cost Price in Reference Currency |
| `amoc_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `lcos_refc` | `float` | `float` |  |  | CC: Landed Cost Amount in Reference Currency |
| `pris_refc` | `float` | `float` |  |  | CC: Sales Price in Reference Currency |
| `amos_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `cona_refc` | `float` | `float` |  |  | CC: Contingency Amount in Reference Currency |
| `pric_cntc` | `float` | `float` |  |  | CC: Cost Price in Contract Currency |
| `amoc_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `lcos_cntc` | `float` | `float` |  |  | CC: Landed Cost Amount in Contract Currency |
| `pris_cntc` | `float` | `float` |  |  | CC: Sales Price in Contract Currency |
| `amos_cntc` | `float` | `float` |  |  | CC: Sales Amount in Contract Currency |
| `cona_cntc` | `float` | `float` |  |  | CC: Contingency Amount in Contract Currency |
| `pric_dwhc` | `float` | `float` |  |  | CC: Cost Price in Data Warehouse Currency |
| `amoc_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `lcos_dwhc` | `float` | `float` |  |  | CC: Landed Cost Amount in Data Warehouse Currency |
| `pris_dwhc` | `float` | `float` |  |  | CC: Sales Price in Data Warehouse Currency |
| `amos_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `cona_dwhc` | `float` | `float` |  |  | CC: Contingency Amount in Data Warehouse Currency |
| `quan_ctut` | `float` | `float` |  |  | CC: Labor Hours in General Unit of Hours |
| `cprj_task` | `string` | `varchar` |  |  | CC: Project Labor. Max length: 25 |
| `cprj_cico` | `string` | `varchar` |  |  | CC: Project Sundry. Max length: 25 |
| `task_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Task |
| `cico_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Sundry |
| `quan_base_time` | `float` | `float` |  |  | CC: Labor Hours in General Unit of Hours |
| `cprj_fcmp` | `integer` | `int` |  |  | CC: Project Financial Company |
| `quan_buar` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Area |
| `quan_buvl` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Volume |
| `quan_buln` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Length |
| `quan_bupc` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Piece |
| `quan_buwg` | `float` | `float` |  |  | CC: Net Quantity in Base Unit Weight |
| `quan_invu` | `float` | `float` |  |  | CC: Net Quantity in Inventory Unit |
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
