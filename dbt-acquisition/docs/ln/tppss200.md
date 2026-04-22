# tppss200

LN tppss200 Activities table, Generated 2026-04-10T19:42:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cpla`, `cact` |
| **Column count** | 167 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `actp` | `integer` | `int` |  |  | Activity Position |
| `levl` | `integer` | `int` |  |  | Level Number |
| `cloc` | `string` | `varchar` |  |  | Project Location. Max length: 6 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `tact` | `integer` | `int` |  |  | Activity Type. Allowed values: 1, 2, 3, 4, 5 |
| `tact_kw` | `string` | `varchar` |  |  | Activity Type (keyword). Allowed values: tppdm.tact.wbsel, tppdm.tact.cosac, tppdm.tact.plapa, tppdm.tact.worpa, tppdm.tact.milst |
| `capt` | `integer` | `int` |  |  | Critical Capacity Type. Allowed values: 1, 2, 3, 4, 5, 20 |
| `capt_kw` | `string` | `varchar` |  |  | Critical Capacity Type (keyword). Allowed values: tppdm.capt.tasks, tppdm.capt.materials, tppdm.capt.equipment, tppdm.capt.subcontracting, tppdm.capt.indirect, tppdm.capt.na |
| `pact` | `string` | `varchar` |  |  | Parent Activity. Max length: 16 |
| `dfac` | `string` | `varchar` |  |  | Derived from Standard Activity. Max length: 16 |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `prsp` | `float` | `float` |  |  | Production Rate |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `mevl` | `integer` | `int` |  |  | Earned Value Method. Allowed values: 1, 2, 3, 4, 5 |
| `mevl_kw` | `string` | `varchar` |  |  | Earned Value Method (keyword). Allowed values: tppss.mevl.milst, tppss.mevl.dsepr, tppss.mevl.perco, tppss.mevl.levef, tppss.mevl.appor |
| `lact` | `string` | `varchar` |  |  | Linked Activity. Max length: 16 |
| `litm` | `string` | `varchar` |  |  | Customized Item. Max length: 47 |
| `pcom` | `float` | `float` |  |  | Percentage Completed |
| `stat` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2, 3 |
| `stat_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tpptc.stat.free, tpptc.stat.actual, tpptc.stat.definite |
| `coel` | `string` | `varchar` |  |  | OBS Element. Max length: 8 |
| `dwar` | `string` | `varchar` |  |  | Project Warehouse. Max length: 6 |
| `lvps` | `integer` | `int` |  |  | Physical Progress Registration. Allowed values: 1, 2, 20 |
| `lvps_kw` | `string` | `varchar` |  |  | Physical Progress Registration (keyword). Allowed values: tppss.lvps.activity, tppss.lvps.activit.costctr, tppss.lvps.na |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `cctr` | `integer` | `int` |  |  | Cost Control. Allowed values: 1, 2 |
| `cctr_kw` | `string` | `varchar` |  |  | Cost Control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ssdt` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `sfdt` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `esdt` | `timestamp` | `timestamp_ntz` |  |  | Earliest Start Date |
| `lsdt` | `timestamp` | `timestamp_ntz` |  |  | Latest Start Date |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Earliest Finish Date |
| `lfdt` | `timestamp` | `timestamp_ntz` |  |  | Latest Finish Date |
| `fref` | `float` | `float` |  |  | Free Float |
| `totf` | `float` | `float` |  |  | Total Float |
| `ffhr` | `float` | `float` |  |  | Free Float in Hours |
| `tfhr` | `float` | `float` |  |  | Total Float in Hours |
| `ccal` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `avtp` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `dura` | `float` | `float` |  |  | Duration |
| `tmud` | `string` | `varchar` |  |  | Time Unit Duration. Max length: 3 |
| `asdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date |
| `afdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Finish Date |
| `cnst` | `integer` | `int` |  |  | Constraint Type. Allowed values: 10, 20, 30, 40, 50, 60, 70, 80 |
| `cnst_kw` | `string` | `varchar` |  |  | Constraint Type (keyword). Allowed values: tppss.cnst.as.soon.as.poss, tppss.cnst.as.late.as.poss, tppss.cnst.must.start.on, tppss.cnst.must.finish.on, tppss.cnst.start.no.earl, tppss.cnst.start.no.later, tppss.cnst.finish.no.earl, tppss.cnst.finish.no.later |
| `cnsd` | `timestamp` | `timestamp_ntz` |  |  | Constraint Date |
| `ddln` | `timestamp` | `timestamp_ntz` |  |  | Deadline Date |
| `qutm` | `float` | `float` |  |  | Number of Time Units |
| `sdch` | `integer` | `int` |  |  | Scheduled Start Date is Changed. Allowed values: 1, 2 |
| `sdch_kw` | `string` | `varchar` |  |  | Scheduled Start Date is Changed (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `qans` | `float` | `float` |  |  | Physical Progress Quantity |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sacu` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `rtsa_1` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rtsa_2` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rtsa_3` | `float` | `float` |  |  | Currency Rate Sales Price |
| `rfsa_1` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rfsa_2` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rfsa_3` | `integer` | `int` |  |  | Rate Factor Sales Price |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `setl` | `integer` | `int` |  |  | To be Settled. Allowed values: 1, 2 |
| `setl_kw` | `string` | `varchar` |  |  | To be Settled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `secu` | `string` | `varchar` |  |  | Settlement Currency. Max length: 3 |
| `prss` | `float` | `float` |  |  | Settlement Price |
| `rtse_1` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rtse_2` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rtse_3` | `float` | `float` |  |  | Currency Rate Settlement Price |
| `rfse_1` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rfse_2` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rfse_3` | `integer` | `int` |  |  | Rate Factor Settlement Price |
| `rdse` | `timestamp` | `timestamp_ntz` |  |  | Rate Date Settlement |
| `cspt` | `integer` | `int` |  |  | Progress for Invoice. Allowed values: 1, 2, 3 |
| `cspt_kw` | `string` | `varchar` |  |  | Progress for Invoice (keyword). Allowed values: tpptc.cspt.direct, tpptc.cspt.indirect, tpptc.cspt.na |
| `gris` | `integer` | `int` |  |  | Group Indirect Work |
| `prin` | `integer` | `int` |  |  | Progress based Invoicing. Allowed values: 1, 2, 3, 4 |
| `prin_kw` | `string` | `varchar` |  |  | Progress based Invoicing (keyword). Allowed values: tppdm.prin.yes, tppdm.prin.no, tppdm.prin.project.stage, tppdm.prin.na |
| `hbyn` | `integer` | `int` |  |  | Use Holdback. Allowed values: 1, 2 |
| `hbyn_kw` | `string` | `varchar` |  |  | Use Holdback (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `mamt` | `float` | `float` |  |  | Milestone Amount |
| `wast` | `integer` | `int` |  |  | Work Authorization Status. Allowed values: 1, 2, 3, 4, 5 |
| `wast_kw` | `string` | `varchar` |  |  | Work Authorization Status (keyword). Allowed values: tppdm.wast.free, tppdm.wast.onhold, tppdm.wast.released, tppdm.wast.finished, tppdm.wast.closed |
| `rcod` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `ohdt` | `timestamp` | `timestamp_ntz` |  |  | On Hold Date |
| `rfac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `freq` | `float` | `float` |  |  | Frequency |
| `afrt` | `integer` | `int` |  |  | Apply Frequency to. Allowed values: 1, 2, 3, 10 |
| `afrt_kw` | `string` | `varchar` |  |  | Apply Frequency to (keyword). Allowed values: tppdm.afrt.multiply, tppdm.afrt.create.act, tppdm.afrt.create.ord, tppdm.afrt.na |
| `pwar` | `string` | `varchar` |  |  | Priority Supply Warehouse. Max length: 6 |
| `capa` | `integer` | `int` |  |  | Capital Activity. Allowed values: 1, 2 |
| `capa_kw` | `string` | `varchar` |  |  | Capital Activity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `anbr` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `prst` | `float` | `float` |  |  | Start Percentage |
| `prnd` | `float` | `float` |  |  | End Percentage |
| `prms` | `float` | `float` |  |  | Milestone Percentage |
| `llnr` | `integer` | `int` |  |  | Low Level Number |
| `scmd` | `integer` | `int` |  |  | Schedule Mode. Allowed values: 0, 5, 10 |
| `scmd_kw` | `string` | `varchar` |  |  | Schedule Mode (keyword). Allowed values: , tppss.scmd.manual, tppss.scmd.auto |
| `acmn` | `string` | `varchar` |  |  | Activity Manager. Max length: 9 |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `addr` | `string` | `varchar` |  |  | Address. Max length: 9 |
| `glat` | `float` | `float` |  |  | GPS Latitude (WGS84) |
| `glon` | `float` | `float` |  |  | GPS Longitude (WGS84) |
| `eoty` | `integer` | `int` |  |  | Business Process Type. Allowed values: 10, 20, 30, 40, 50, 60, 255 |
| `eoty_kw` | `string` | `varchar` |  |  | Business Process Type (keyword). Allowed values: tpplm.eoty.ecn, tpplm.eoty.eco, tpplm.eoty.ecp, tpplm.eoty.appr.proc, tpplm.eoty.ecr, tpplm.eoty.npi, tpplm.eoty.na |
| `plmc` | `integer` | `int` |  |  | PLM Company |
| `pbpt__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Process Type - base language. Max length: 30 |
| `plmp` | `string` | `varchar` |  |  | PLM Project. Max length: 15 |
| `rout` | `string` | `varchar` |  |  | Workflow Template. Max length: 30 |
| `eoid` | `string` | `varchar` |  |  | PLM Business Process. Max length: 30 |
| `rent` | `integer` | `int` |  |  | Rental Template. Allowed values: 1, 2 |
| `rent_kw` | `string` | `varchar` |  |  | Rental Template (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_acrb` | `float` | `float` |  |  | Accrual Balance |
| `cdf_awip` | `float` | `float` |  |  | WIP Balance |
| `cdf_camt` | `float` | `float` |  |  | Commitment to Date |
| `cdf_depm` | `integer` | `int` |  |  | Show in d/EPM. Allowed values: 1, 2 |
| `cdf_depm_kw` | `string` | `varchar` |  |  | Show in d/EPM (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_uamt` | `float` | `float` |  |  | Unapproved Commitment |
| `cprj_cpla_pact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_lact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_cloc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm742 Locations by Project |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `dfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm110 Standard Activities |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `litm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `dwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `avtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `tmud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `sacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `secu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `pwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `acmn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `addr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `coel_rcmp` | `integer` | `int` |  |  | CC: Reference Company of OBS Element |
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
