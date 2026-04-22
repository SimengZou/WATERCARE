# tppdm110

LN tppdm110 Standard Activities table, Generated 2026-04-10T19:41:58Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cact` |
| **Column count** | 52 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `tact` | `integer` | `int` |  |  | Activity Type. Allowed values: 1, 2, 3, 4, 5 |
| `tact_kw` | `string` | `varchar` |  |  | Activity Type (keyword). Allowed values: tppdm.tact.wbsel, tppdm.tact.cosac, tppdm.tact.plapa, tppdm.tact.worpa, tppdm.tact.milst |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `prsp` | `float` | `float` |  |  | Production Rate |
| `lvps` | `integer` | `int` |  |  | Phys. Progress Registr.. Allowed values: 1, 2, 20 |
| `lvps_kw` | `string` | `varchar` |  |  | Phys. Progress Registr. (keyword). Allowed values: tppss.lvps.activity, tppss.lvps.activit.costctr, tppss.lvps.na |
| `prin` | `integer` | `int` |  |  | Progress based Invoicing. Allowed values: 1, 2, 3, 4 |
| `prin_kw` | `string` | `varchar` |  |  | Progress based Invoicing (keyword). Allowed values: tppdm.prin.yes, tppdm.prin.no, tppdm.prin.project.stage, tppdm.prin.na |
| `cspt` | `integer` | `int` |  |  | Progress for Invoice. Allowed values: 1, 2, 3 |
| `cspt_kw` | `string` | `varchar` |  |  | Progress for Invoice (keyword). Allowed values: tpptc.cspt.direct, tpptc.cspt.indirect, tpptc.cspt.na |
| `setl` | `integer` | `int` |  |  | To be Settled. Allowed values: 1, 2 |
| `setl_kw` | `string` | `varchar` |  |  | To be Settled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 0, 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `mevl` | `integer` | `int` |  |  | Earned Value Method. Allowed values: 1, 2, 3, 4, 5 |
| `mevl_kw` | `string` | `varchar` |  |  | Earned Value Method (keyword). Allowed values: tppss.mevl.milst, tppss.mevl.dsepr, tppss.mevl.perco, tppss.mevl.levef, tppss.mevl.appor |
| `capt` | `integer` | `int` |  |  | Critical Capacity Type. Allowed values: 1, 2, 3, 4, 5, 20 |
| `capt_kw` | `string` | `varchar` |  |  | Critical Capacity Type (keyword). Allowed values: tppdm.capt.tasks, tppdm.capt.materials, tppdm.capt.equipment, tppdm.capt.subcontracting, tppdm.capt.indirect, tppdm.capt.na |
| `rfac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `freq` | `float` | `float` |  |  | Frequency |
| `afrt` | `integer` | `int` |  |  | Apply Frequency to. Allowed values: 1, 2, 3, 10 |
| `afrt_kw` | `string` | `varchar` |  |  | Apply Frequency to (keyword). Allowed values: tppdm.afrt.multiply, tppdm.afrt.create.act, tppdm.afrt.create.ord, tppdm.afrt.na |
| `prst` | `float` | `float` |  |  | Start Percentage |
| `prnd` | `float` | `float` |  |  | End Percentage |
| `prms` | `float` | `float` |  |  | Milestone Percentage |
| `eoty` | `integer` | `int` |  |  | Business Process Type. Allowed values: 10, 20, 30, 40, 50, 60, 255 |
| `eoty_kw` | `string` | `varchar` |  |  | Business Process Type (keyword). Allowed values: tpplm.eoty.ecn, tpplm.eoty.eco, tpplm.eoty.ecp, tpplm.eoty.appr.proc, tpplm.eoty.ecr, tpplm.eoty.npi, tpplm.eoty.na |
| `plmc` | `integer` | `int` |  |  | PLM Company |
| `pbpt__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Process Type - base language. Max length: 30 |
| `plmp` | `string` | `varchar` |  |  | PLM Project. Max length: 15 |
| `rout` | `string` | `varchar` |  |  | Workflow Template. Max length: 30 |
| `rent` | `integer` | `int` |  |  | Rental Template. Allowed values: 1, 2 |
| `rent_kw` | `string` | `varchar` |  |  | Rental Template (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `plmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
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
