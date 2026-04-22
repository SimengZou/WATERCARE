# tirou001

LN tirou001 Work Center table, Generated 2026-04-10T19:41:48Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tirou001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwoc` |
| **Column count** | 81 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwoc` | `string` | `varchar` | `PK` | `not_null` | (required) Work Center. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `kowc` | `integer` | `int` |  |  | Work Center Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 30 |
| `kowc_kw` | `string` | `varchar` |  |  | Work Center Type (keyword). Allowed values: tckowc.main.wc, tckowc.sub.wc, tckowc.subcontracting, tckowc.cost, tckowc.buffer, tckowc.line.station, tckowc.work.cell, tckowc.repair.cell, tckowc.asm.cell, tckowc.calc.office, tckowc.transformation |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `stty` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `stty_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tirou.stty.line.station, tirou.stty.buffer, tirou.stty.not.appl |
| `bfty` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `bfty_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tirou.bfty.rac, tirou.bfty.fifo, tirou.bfty.not.appl |
| `mnwc` | `string` | `varchar` |  |  | Parent Work Center. Max length: 6 |
| `scid` | `integer` | `int` |  |  | Subcontractor Independent. Allowed values: 1, 2 |
| `scid_kw` | `string` | `varchar` |  |  | Subcontractor Independent (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpid` | `string` | `varchar` |  |  | Subcontractor. Max length: 9 |
| `oprc` | `string` | `varchar` |  |  | Operation Rate Code. Max length: 6 |
| `wttm` | `float` | `float` |  |  | Wait Time |
| `mvtm` | `float` | `float` |  |  | Move Time |
| `wcru` | `float` | `float` |  |  | Basic Week Capacity by Resource Unit |
| `dcru` | `float` | `float` |  |  | Basic Day Capacity by Resource Unit |
| `wipw` | `string` | `varchar` |  |  | Shop Floor Warehouse. Max length: 6 |
| `bfem` | `string` | `varchar` |  |  | Default Labor Resource. Max length: 9 |
| `crmp` | `integer` | `int` |  |  | Critical in Master Planning. Allowed values: 1, 2 |
| `crmp_kw` | `string` | `varchar` |  |  | Critical in Master Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `obfu` | `integer` | `int` |  |  | Obsolete |
| `norp` | `integer` | `int` |  |  | Obsolete |
| `swct` | `integer` | `int` |  |  | Shared Work Center Type. Allowed values: 5, 10, 15 |
| `swct_kw` | `string` | `varchar` |  |  | Shared Work Center Type (keyword). Allowed values: tirou.swct.non.shared, tirou.swct.primary, tirou.swct.secondary |
| `swcc` | `integer` | `int` |  |  | Shared Work Center Primary Company |
| `dque` | `float` | `float` |  |  | Desired Queue |
| `qutm` | `float` | `float` |  |  | Queue Time |
| `nomc` | `float` | `float` |  |  | Number of Machines |
| `noop` | `float` | `float` |  |  | Available Labor Resources (FTE) |
| `tuni` | `integer` | `int` |  |  | Time Unit. Allowed values: 5, 10 |
| `tuni_kw` | `string` | `varchar` |  |  | Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `prin` | `integer` | `int` |  |  | Print Instructions. Allowed values: 1, 2, 3 |
| `prin_kw` | `string` | `varchar` |  |  | Print Instructions (keyword). Allowed values: tiprin.yes, tiprin.no, tiprin.noappl |
| `ctwc` | `string` | `varchar` |  |  | Costing Work Center. Max length: 6 |
| `crty` | `integer` | `int` |  |  | Constraint Type. Allowed values: 1, 2, 3 |
| `crty_kw` | `string` | `varchar` |  |  | Constraint Type (keyword). Allowed values: ticrty.not.appl, ticrty.time.fence, ticrty.plan.horizon |
| `crtf` | `integer` | `int` |  |  | Constraint Time Fence |
| `ccap` | `integer` | `int` |  |  | Show in Resource Master Plan. Allowed values: 1, 2 |
| `ccap_kw` | `string` | `varchar` |  |  | Show in Resource Master Plan (keyword). Allowed values: tcccap.man.capacity, tcccap.mach.capacity |
| `upcs` | `integer` | `int` |  |  | Use rough PCS capacity requirement. Allowed values: 1, 2 |
| `upcs_kw` | `string` | `varchar` |  |  | Use rough PCS capacity requirement (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lbdv` | `string` | `varchar` |  |  | Device for Printing Labels. Max length: 14 |
| `pddp` | `string` | `varchar` |  |  | Production Department. Max length: 6 |
| `usco` | `integer` | `int` |  |  | Use as Calculation Office. Allowed values: 1, 2 |
| `usco_kw` | `string` | `varchar` |  |  | Use as Calculation Office (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mcgr` | `integer` | `int` |  |  | Machine Capacity Groups. Allowed values: 1, 2 |
| `mcgr_kw` | `string` | `varchar` |  |  | Machine Capacity Groups (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `_rows` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `rows_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `rpwc` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `wcpg` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `rprd` | `integer` | `int` |  |  | Report Product. Allowed values: 1, 2, 10, 20, 30 |
| `rprd_kw` | `string` | `varchar` |  |  | Report Product (keyword). Allowed values: tirep.prod.manual, tirep.prod.automatic, tirep.prod.machine, tirep.prod.by.mes, tirep.prod.mach.group |
| `rlbr` | `integer` | `int` |  |  | Report Labor Hours. Allowed values: 1, 2, 30, 40, 50, 80, 90 |
| `rlbr_kw` | `string` | `varchar` |  |  | Report Labor Hours (keyword). Allowed values: tirep.cons.backflush, tirep.cons.manual, tirep.cons.machine, tirep.cons.by.mes, tirep.cons.mach.group, tirep.cons.undefined, tirep.cons.not.applicable |
| `cpgr` | `string` | `varchar` |  |  | Capacity Group. Max length: 6 |
| `plgr` | `string` | `varchar` |  |  | Plan Group. Max length: 6 |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `mnwc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `oprc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ticpr050 Operation Rate Codes |
| `wipw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `bfem_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ctwc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `pddp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou200 Production Departments |
| `rpwc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `wcpg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirpt042 Work Cell Plan Groups |
| `pddp_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Production Department |
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
