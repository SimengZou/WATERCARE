# tppdm002

LN tppdm002 Map Session Options by User table, Generated 2026-04-10T19:41:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm002` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `loco`, `maps` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `loco` | `string` | `varchar` | `PK` | `not_null` | (required) User. Max length: 16 |
| `maps` | `integer` | `int` | `PK` | `not_null` | (required) Maps. Allowed values: 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 |
| `maps_kw` | `string` | `varchar` |  |  | Maps (keyword). Allowed values: tppdm.maps.obs, tppdm.maps.proj.scope.defn, tppdm.maps.topdown.budg, tppdm.maps.timephased.budg, tppdm.maps.control.budg, tppdm.maps.bottomup.budg, tppdm.maps.bca, tppdm.maps.hist.budg, tppdm.maps.cost.forecast, tppdm.maps.reve.forecast, tppdm.maps.pur.order, tppdm.maps.wrh.order, tppdm.maps.ord.balance, tppdm.maps.del.orders, tppdm.maps.progress, tppdm.maps.commitments, tppdm.maps.costs, tppdm.maps.revenues, tppdm.maps.hist.costs, tppdm.maps.hist.revenues, tppdm.maps.conf.commitment, tppdm.maps.conf.costs, tppdm.maps.conf.revenues, tppdm.maps.prog.invo, tppdm.maps.cost.plus, tppdm.maps.control.inq, tppdm.maps.perf.meas, tppdm.maps.estimating, tppdm.maps.prj.delv, tppdm.maps.bids, tppdm.maps.resched.message |
| `mpss` | `integer` | `int` |  |  | Default Start up (Map - Session). Allowed values: 1, 2, 3 |
| `mpss_kw` | `string` | `varchar` |  |  | Default Start up (Map - Session) (keyword). Allowed values: tppdm.mpss.map, tppdm.mpss.sess, tppdm.mpss.map.sess |
| `mpvw` | `integer` | `int` |  |  | Default Main View. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| `mpvw_kw` | `string` | `varchar` |  |  | Default Main View (keyword). Allowed values: tppdm.mpvw.project, tppdm.mpvw.element, tppdm.mpvw.activity, tppdm.mpvw.extension, tppdm.mpvw.obs, tppdm.mpvw.primary, tppdm.mpvw.sort, tppdm.mpvw.asta, tppdm.mpvw.astb, tppdm.mpvw.astc, tppdm.mpvw.astd, tppdm.mpvw.aste, tppdm.mpvw.astf, tppdm.mpvw.astg, tppdm.mpvw.ofbp, tppdm.mpvw.structure, tppdm.mpvw.item |
| `sses` | `integer` | `int` |  |  | Start up Session. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `sses_kw` | `string` | `varchar` |  |  | Start up Session (keyword). Allowed values: tppdm.sses.project, tppdm.sses.element, tppdm.sses.element.quan, tppdm.sses.activity, tppdm.sses.extension, tppdm.sses.cost.type, tppdm.sses.cost.comp, tppdm.sses.materials, tppdm.sses.tasks, tppdm.sses.equipment, tppdm.sses.subcontracting, tppdm.sses.indirect, tppdm.sses.obs, tppdm.sses.not.applicable, tppdm.sses.relation, tppdm.sses.mat.ctrl, tppdm.sses.lab.ctrl, tppdm.sses.eqp.ctrl, tppdm.sses.subcon.ctrl, tppdm.sses.snd.ctrl, tppdm.sses.subc.hours |
| `ssvw` | `integer` | `int` |  |  | Session View. Allowed values: 1, 2, 4, 7, 8, 10, 11, 12, 13 |
| `ssvw_kw` | `string` | `varchar` |  |  | Session View (keyword). Allowed values: tppdm.ssvw.cprj.date, tppdm.ssvw.project, tppdm.ssvw.year.period, tppdm.ssvw.cprj, tppdm.ssvw.cprj.costobject, tppdm.ssvw.structure, tppdm.ssvw.extension, tppdm.ssvw.ofbp, tppdm.ssvw.item |
| `tprf` | `integer` | `int` |  |  | Type of Profile. Allowed values: 5, 10 |
| `tprf_kw` | `string` | `varchar` |  |  | Type of Profile (keyword). Allowed values: tcmcs.tprf.user, tcmcs.tprf.template |
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
