# tppdm001

LN tppdm001 User Profiles table, Generated 2026-04-10T19:41:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `loco` |
| **Column count** | 106 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `loco` | `string` | `varchar` | `PK` | `not_null` | (required) User. Max length: 16 |
| `udpr` | `integer` | `int` |  |  | Use Default Project. Allowed values: 1, 2 |
| `udpr_kw` | `string` | `varchar` |  |  | Use Default Project (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `udsp` | `integer` | `int` |  |  | Use Default Element. Allowed values: 1, 2 |
| `udsp_kw` | `string` | `varchar` |  |  | Use Default Element (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `udpl` | `integer` | `int` |  |  | Use Default Project Plan. Allowed values: 1, 2 |
| `udpl_kw` | `string` | `varchar` |  |  | Use Default Project Plan (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpla` | `string` | `varchar` |  |  | Project Plan. Max length: 3 |
| `udac` | `integer` | `int` |  |  | Use Default Activity. Allowed values: 1, 2 |
| `udac_kw` | `string` | `varchar` |  |  | Use Default Activity (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `whng` | `string` | `varchar` |  |  | PRP Warehouse Orders Number Group. Max length: 3 |
| `whsr` | `string` | `varchar` |  |  | PRP Warehouse Order Series. Max length: 8 |
| `pung` | `string` | `varchar` |  |  | PRP Purchase Orders Number Group. Max length: 3 |
| `pusr` | `string` | `varchar` |  |  | PRP Purchase Order Series. Max length: 8 |
| `song` | `string` | `varchar` |  |  | Service Orders Number Group. Max length: 3 |
| `sosr` | `string` | `varchar` |  |  | Service Order Series. Max length: 8 |
| `erng` | `string` | `varchar` |  |  | Planned Equipment Request Number Group. Max length: 3 |
| `ersr` | `string` | `varchar` |  |  | Planned Equipment Request Series. Max length: 8 |
| `ctng` | `string` | `varchar` |  |  | Contracts Number Group. Max length: 3 |
| `ctsr` | `string` | `varchar` |  |  | Contract Series. Max length: 8 |
| `ngrp` | `string` | `varchar` |  |  | Project Number Group. Max length: 3 |
| `prsr` | `string` | `varchar` |  |  | Project Series. Max length: 8 |
| `ests` | `string` | `varchar` |  |  | Estimate Series. Max length: 8 |
| `serr` | `string` | `varchar` |  |  | Order Series for PCS Project. Max length: 8 |
| `dsct` | `integer` | `int` |  |  | Default Start up Cost Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `dsct_kw` | `string` | `varchar` |  |  | Default Start up Cost Type (keyword). Allowed values: tppdm.cotp.tasks, tppdm.cotp.materials, tppdm.cotp.equipment, tppdm.cotp.subcontracting, tppdm.cotp.indirect, tppdm.cotp.overhead |
| `asct` | `integer` | `int` |  |  | Alternate Start up Cost Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `asct_kw` | `string` | `varchar` |  |  | Alternate Start up Cost Type (keyword). Allowed values: tppdm.cotp.tasks, tppdm.cotp.materials, tppdm.cotp.equipment, tppdm.cotp.subcontracting, tppdm.cotp.indirect, tppdm.cotp.overhead |
| `mpvw` | `integer` | `int` |  |  | Default Main View. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| `mpvw_kw` | `string` | `varchar` |  |  | Default Main View (keyword). Allowed values: tppdm.mpvw.project, tppdm.mpvw.element, tppdm.mpvw.activity, tppdm.mpvw.extension, tppdm.mpvw.obs, tppdm.mpvw.primary, tppdm.mpvw.sort, tppdm.mpvw.asta, tppdm.mpvw.astb, tppdm.mpvw.astc, tppdm.mpvw.astd, tppdm.mpvw.aste, tppdm.mpvw.astf, tppdm.mpvw.astg, tppdm.mpvw.ofbp, tppdm.mpvw.structure, tppdm.mpvw.item |
| `olvl` | `integer` | `int` |  |  | Opening Level |
| `tpte` | `string` | `varchar` |  |  | Project Template. Max length: 9 |
| `udev` | `integer` | `int` |  |  | Use Default Estimate Version. Allowed values: 1, 2 |
| `udev_kw` | `string` | `varchar` |  |  | Use Default Estimate Version (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `vers` | `string` | `varchar` |  |  | Estimate Version. Max length: 3 |
| `tgrp` | `string` | `varchar` |  |  | Template Number Group. Max length: 3 |
| `tpsr` | `string` | `varchar` |  |  | Template Series. Max length: 8 |
| `dfet` | `integer` | `int` |  |  | Default Estimate Type. Allowed values: 1, 2 |
| `dfet_kw` | `string` | `varchar` |  |  | Default Estimate Type (keyword). Allowed values: tpest.etyp.topdown, tpest.etyp.bottomup |
| `dflv` | `integer` | `int` |  |  | Default Level. Allowed values: 1, 2, 3, 4 |
| `dflv_kw` | `string` | `varchar` |  |  | Default Level (keyword). Allowed values: tpest.levl.total, tpest.levl.cost.type, tpest.levl.other.struct, tpest.levl.detail |
| `dcty` | `integer` | `int` |  |  | Default Cost Type. Allowed values: 1, 2, 3, 4, 5, 6, 15, 255 |
| `dcty_kw` | `string` | `varchar` |  |  | Default Cost Type (keyword). Allowed values: tppdm.cotr.tasks, tppdm.cotr.materials, tppdm.cotr.equipment, tppdm.cotr.subcontracting, tppdm.cotr.indirect, tppdm.cotr.refact, tppdm.cotr.composite, tppdm.cotr.not.applicable |
| `prim` | `integer` | `int` |  |  | Pricing Method. Allowed values: 1, 2, 3 |
| `prim_kw` | `string` | `varchar` |  |  | Pricing Method (keyword). Allowed values: tpest.prim.mark.up, tpest.prim.target.pricing, tpest.prim.target.amount |
| `delt` | `integer` | `int` |  |  | Default Estimate Line Type. Allowed values: 1, 2 |
| `delt_kw` | `string` | `varchar` |  |  | Default Estimate Line Type (keyword). Allowed values: tpest.cspt.direct, tpest.cspt.indirect |
| `mnvf` | `integer` | `int` |  |  | Verify Online. Allowed values: 1, 2, 3 |
| `mnvf_kw` | `string` | `varchar` |  |  | Verify Online (keyword). Allowed values: tpest.veon.no.online, tpest.veon.field.level, tpest.veon.record.level |
| `aagt` | `integer` | `int` |  |  | Auto Aggregate Totals. Allowed values: 1, 2 |
| `aagt_kw` | `string` | `varchar` |  |  | Auto Aggregate Totals (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `potp` | `string` | `varchar` |  |  | Project Procedure. Max length: 3 |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `rlcr` | `integer` | `int` |  |  | Can Release Credit Held Orders. Allowed values: 1, 2 |
| `rlcr_kw` | `string` | `varchar` |  |  | Can Release Credit Held Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rlgn` | `integer` | `int` |  |  | Can Release Generally Held Orders. Allowed values: 1, 2 |
| `rlgn_kw` | `string` | `varchar` |  |  | Can Release Generally Held Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tprf` | `integer` | `int` |  |  | Type of Profile. Allowed values: 5, 10 |
| `tprf_kw` | `string` | `varchar` |  |  | Type of Profile (keyword). Allowed values: tcmcs.tprf.user, tcmcs.tprf.template |
| `sdnr` | `string` | `varchar` |  |  | Scope Document Number Group. Max length: 3 |
| `sdsr` | `string` | `varchar` |  |  | Scope Document Series. Max length: 8 |
| `apnr` | `string` | `varchar` |  |  | Application for Payment Number Group. Max length: 3 |
| `apsr` | `string` | `varchar` |  |  | Application for Payment Series. Max length: 8 |
| `prcv` | `integer` | `int` |  |  | Prefill Certified Values based on Applied Values. Allowed values: 0, 1, 2 |
| `prcv_kw` | `string` | `varchar` |  |  | Prefill Certified Values based on Applied Values (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `svsr` | `string` | `varchar` |  |  | Scope Variation Series. Max length: 8 |
| `conr` | `string` | `varchar` |  |  | Change Order Number Group. Max length: 3 |
| `cosr` | `string` | `varchar` |  |  | Change Order Series. Max length: 8 |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_vers_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpest100 Estimate Versions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm090 Standard Elements |
| `whng_whsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `whng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `pung_pusr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `pung_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `song_sosr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `song_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `erng_ersr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `erng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ctng_ctsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ctng_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngrp_prsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngrp_ests_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `tpte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `tgrp_tpsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `tgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `potp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm680 Project Procedures |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `sdnr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `apnr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
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
