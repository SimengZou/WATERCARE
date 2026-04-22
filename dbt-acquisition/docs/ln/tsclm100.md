# tsclm100

LN tsclm100 Calls table, Generated 2026-04-10T19:42:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsclm100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ccll` |
| **Column count** | 205 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ccll` | `string` | `varchar` | `PK` | `not_null` | (required) Call. Max length: 9 |
| `cllo` | `integer` | `int` |  |  | Origin. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40 |
| `cllo_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tsclm.cllo.telep, tsclm.cllo.fax, tsclm.cllo.modem, tsclm.cllo.letter, tsclm.cllo.internal, tsclm.cllo.email, tsclm.cllo.other, tsclm.cllo.internet |
| `fcll` | `string` | `varchar` |  |  | Follow-up Call. Max length: 9 |
| `pcll` | `string` | `varchar` |  |  | Parent Call. Max length: 9 |
| `rsvo` | `string` | `varchar` |  |  | Related Service Order. Max length: 9 |
| `rfco` | `string` | `varchar` |  |  | Related Field Change Order. Max length: 9 |
| `rfcl` | `integer` | `int` |  |  | Related Field Change Order Line |
| `ccty` | `string` | `varchar` |  |  | Country. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to BP. Max length: 9 |
| `soad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `socn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Location Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Location Contact. Max length: 9 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sern` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `ubbp` | `string` | `varchar` |  |  | In Use-by Business Partner. Max length: 9 |
| `ubad` | `string` | `varchar` |  |  | In Use-by Business Partner Address. Max length: 9 |
| `ubcn` | `string` | `varchar` |  |  | In Use-by Business Partner Contact. Max length: 9 |
| `ubop` | `string` | `varchar` |  |  | In Use-by Operator Contact. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone. Max length: 32 |
| `ccln__en_us` | `string` | `varchar` |  | `not_null` | (required) BP Call Number - base language. Max length: 30 |
| `shdp__en_us` | `string` | `varchar` |  | `not_null` | (required) Solution Description - base language. Max length: 50 |
| `shpd__en_us` | `string` | `varchar` |  | `not_null` | (required) Call Description - base language. Max length: 50 |
| `csgr` | `string` | `varchar` |  |  | Service Item Group. Max length: 6 |
| `cgrp` | `string` | `varchar` |  |  | Call Group1. Max length: 3 |
| `scgr` | `string` | `varchar` |  |  | Call Group2. Max length: 3 |
| `crtc` | `string` | `varchar` |  |  | Response Type. Max length: 3 |
| `prpr` | `integer` | `int` |  |  | Problem Priority |
| `cupr` | `integer` | `int` |  |  | Business Partner Priority |
| `obpr` | `integer` | `int` |  |  | Serialized Item Priority |
| `rprl` | `string` | `varchar` |  |  | Reported Problem. Max length: 10 |
| `expr` | `string` | `varchar` |  |  | Expected Problem. Max length: 10 |
| `espr` | `string` | `varchar` |  |  | Actual Problem. Max length: 10 |
| `exsl` | `string` | `varchar` |  |  | Expected Solution. Max length: 10 |
| `sltn` | `string` | `varchar` |  |  | Actual Solution. Max length: 10 |
| `rqct` | `string` | `varchar` |  |  | Required Activity. Max length: 16 |
| `camr` | `string` | `varchar` |  |  | Master Routing. Max length: 16 |
| `actt` | `string` | `varchar` |  |  | Activity Taken. Max length: 16 |
| `bdfx` | `integer` | `int` |  |  | Bad Fix. Allowed values: 1, 2 |
| `bdfx_kw` | `string` | `varchar` |  |  | Bad Fix (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `encl` | `integer` | `int` |  |  | Number of Enclosures |
| `atml` | `float` | `float` |  |  | Actual Time Left |
| `inrt` | `float` | `float` |  |  | Initial Time Left |
| `adur` | `float` | `float` |  |  | Duration |
| `appo` | `integer` | `int` |  |  | Appointment. Allowed values: 1, 2 |
| `appo_kw` | `string` | `varchar` |  |  | Appointment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clna__en_us` | `string` | `varchar` |  | `not_null` | (required) Caller's Name - base language. Max length: 35 |
| `cinv` | `string` | `varchar` |  |  | Invoice Interval. Max length: 3 |
| `inyn` | `integer` | `int` |  |  | Invoice Created. Allowed values: 1, 2 |
| `inyn_kw` | `string` | `varchar` |  |  | Invoice Created (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wait` | `integer` | `int` |  |  | Waiting. Allowed values: 1, 2 |
| `wait_kw` | `string` | `varchar` |  |  | Waiting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wtds__en_us` | `string` | `varchar` |  | `not_null` | (required) Waiting Description - base language. Max length: 50 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 5, 10, 15, 20, 25, 30 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tsclm.stat.regist, tsclm.stat.assign, tsclm.stat.process, tsclm.stat.transf, tsclm.stat.solved, tsclm.stat.accept |
| `cspc` | `string` | `varchar` |  |  | Call Center. Max length: 6 |
| `cwoc` | `string` | `varchar` |  |  | Support Department. Max length: 6 |
| `emno` | `string` | `varchar` |  |  | Support Engineer. Max length: 9 |
| `cxsc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `crdt` | `date` | `date` |  |  | Creation Date |
| `rpct` | `timestamp` | `timestamp_ntz` |  |  | Reported Time |
| `asct` | `timestamp` | `timestamp_ntz` |  |  | Assessment Time |
| `slct` | `timestamp` | `timestamp_ntz` |  |  | Solution Time |
| `ustm` | `integer` | `int` |  |  | Spent Time |
| `adtm` | `integer` | `int` |  |  | Actual Down Time |
| `rtct` | `timestamp` | `timestamp_ntz` |  |  | Reaction Time |
| `arct` | `timestamp` | `timestamp_ntz` |  |  | Actual Reaction Time |
| `egct` | `timestamp` | `timestamp_ntz` |  |  | Latest Solution Start Time |
| `svct` | `timestamp` | `timestamp_ntz` |  |  | Latest Solution Finish Time |
| `pect` | `timestamp` | `timestamp_ntz` |  |  | Execution Start Time |
| `pfct` | `timestamp` | `timestamp_ntz` |  |  | Actual Finish Time |
| `acct` | `timestamp` | `timestamp_ntz` |  |  | Accepted Time |
| `user` | `string` | `varchar` |  |  | Call Taker. Max length: 12 |
| `blyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bldt` | `timestamp` | `timestamp_ntz` |  |  | Last Unblocked Date |
| `emer` | `integer` | `int` |  |  | Emergency Call. Allowed values: 1, 2 |
| `emer_kw` | `string` | `varchar` |  |  | Emergency Call (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oemn` | `string` | `varchar` |  |  | Service Engineer. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `csco` | `string` | `varchar` |  |  | Coverage Contract. Max length: 9 |
| `pcon` | `string` | `varchar` |  |  | Pricing Contract. Max length: 9 |
| `pcch` | `integer` | `int` |  |  | Pricing Contract Change |
| `pcln` | `integer` | `int` |  |  | Pricing Contract Line |
| `cvtm` | `timestamp` | `timestamp_ntz` |  |  | Coverage Time |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from BP. Max length: 9 |
| `sbct` | `string` | `varchar` |  |  | Subcontractor Agreement. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `rmso` | `string` | `varchar` |  |  | Related Maintenance Sales Order. Max length: 9 |
| `csqu` | `string` | `varchar` |  |  | Related Service Order Quote. Max length: 9 |
| `citm` | `string` | `varchar` |  |  | Search Item. Max length: 47 |
| `cser` | `string` | `varchar` |  |  | Search Item Serial Number. Max length: 30 |
| `cctp` | `string` | `varchar` |  |  | Coverage Type. Max length: 3 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `pcac` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `inby` | `integer` | `int` |  |  | Invoicing By. Allowed values: 5, 10, 99 |
| `inby_kw` | `string` | `varchar` |  |  | Invoicing By (keyword). Allowed values: tcinby.project, tcinby.service, tcinby.not.appl |
| `docn` | `string` | `varchar` |  |  | Turnaround Time Document. Max length: 9 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `guid` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `cact` | `string` | `varchar` |  |  | Obsolete. Max length: 16 |
| `minu` | `integer` | `int` |  |  | Obsolete |
| `rlct` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `susn` | `string` | `varchar` |  |  | Obsolete. Max length: 30 |
| `ctik` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `susr` | `string` | `varchar` |  |  | Obsolete. Max length: 47 |
| `txta` | `integer` | `int` |  |  | Comment |
| `txtd` | `integer` | `int` |  |  | Invoice Text |
| `txte` | `integer` | `int` |  |  | Solution Text |
| `txtf` | `integer` | `int` |  |  | Knowledge Base Solution |
| `txtg` | `integer` | `int` |  |  | Knowledge Base State |
| `txth` | `integer` | `int` |  |  | Knowledge Base Cause |
| `txtk` | `integer` | `int` |  |  | Comment History |
| `fcll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm100 Calls |
| `pcll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm100 Calls |
| `rfco_rfcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc510 Field Change Order Lines |
| `rfco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc500 Field Change Order Headers |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `soad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `socn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `ubbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ubad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ubcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ubop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `csgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm210 Service Item Groups |
| `cgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm050 Call Groups |
| `scgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm050 Call Groups |
| `crtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm020 Response Types |
| `prpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cupr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `obpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `rprl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `expr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `espr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `exsl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm335 Solutions |
| `sltn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm335 Solutions |
| `rqct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `camr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `actt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `cinv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm070 Time Intervals for Invoicing |
| `cspc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `cxsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `oemn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `pcon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `bfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sbct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssbm100 Subcontract Agreement Header |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `citm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `citm_cser_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `docn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcttm100 Turnaround Time Documents |
| `susr_susn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `susr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `repr_wrty` | `integer` | `int` |  |  | CC: Repair Warranty of Calls. Allowed values: 0, 1, 2 |
| `repr_wrty_kw` | `string` | `varchar` |  |  | CC: Repair Warranty of Calls (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
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
