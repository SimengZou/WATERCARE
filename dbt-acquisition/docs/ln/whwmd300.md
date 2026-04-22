# whwmd300

LN whwmd300 Locations table, Generated 2026-04-10T19:42:54Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whwmd300` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `loca` |
| **Column count** | 77 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` | `PK` | `not_null` | (required) Location. Max length: 10 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `strt` | `string` | `varchar` |  |  | Row. Max length: 2 |
| `coln` | `string` | `varchar` |  |  | Level. Max length: 4 |
| `rack` | `string` | `varchar` |  |  | Bin. Max length: 4 |
| `zone` | `string` | `varchar` |  |  | Storage Zone. Max length: 8 |
| `loct` | `integer` | `int` |  |  | Location Type. Allowed values: 1, 3, 4, 5, 6, 7, 20 |
| `loct_kw` | `string` | `varchar` |  |  | Location Type (keyword). Allowed values: whwmd.loct.inspection, whwmd.loct.receiving, whwmd.loct.pick, whwmd.loct.bulk, whwmd.loct.staging, whwmd.loct.reject, whwmd.loct.not.appl |
| `pseq` | `integer` | `int` |  |  | Sequence on Picking List |
| `sseq` | `integer` | `int` |  |  | Sequence on Storage List |
| `ball` | `integer` | `int` |  |  | Blocked for All Transactions. Allowed values: 1, 2 |
| `ball_kw` | `string` | `varchar` |  |  | Blocked for All Transactions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oclo` | `integer` | `int` |  |  | Location Occupied. Allowed values: 1, 2 |
| `oclo_kw` | `string` | `varchar` |  |  | Location Occupied (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inca` | `integer` | `int` |  |  | Infinite Capacity. Allowed values: 1, 2 |
| `inca_kw` | `string` | `varchar` |  |  | Infinite Capacity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inlo` | `integer` | `int` |  |  | Inbound. Allowed values: 1, 2 |
| `inlo_kw` | `string` | `varchar` |  |  | Inbound (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `binb` | `integer` | `int` |  |  | Blocked for Inbound. Allowed values: 1, 2 |
| `binb_kw` | `string` | `varchar` |  |  | Blocked for Inbound (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `outl` | `integer` | `int` |  |  | Outbound. Allowed values: 1, 2 |
| `outl_kw` | `string` | `varchar` |  |  | Outbound (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bout` | `integer` | `int` |  |  | Blocked for Outbound. Allowed values: 1, 2 |
| `bout_kw` | `string` | `varchar` |  |  | Blocked for Outbound (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trfr` | `integer` | `int` |  |  | Transfer (Receipt). Allowed values: 1, 2 |
| `trfr_kw` | `string` | `varchar` |  |  | Transfer (Receipt) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `btrr` | `integer` | `int` |  |  | Blocked for Transfer (Receipt). Allowed values: 1, 2 |
| `btrr_kw` | `string` | `varchar` |  |  | Blocked for Transfer (Receipt) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trto` | `integer` | `int` |  |  | Transfer (Issue). Allowed values: 1, 2 |
| `trto_kw` | `string` | `varchar` |  |  | Transfer (Issue) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `btri` | `integer` | `int` |  |  | Blocked for Transfer (Issue). Allowed values: 1, 2 |
| `btri_kw` | `string` | `varchar` |  |  | Blocked for Transfer (Issue) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aslo` | `integer` | `int` |  |  | Assembly. Allowed values: 1, 2 |
| `aslo_kw` | `string` | `varchar` |  |  | Assembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bass` | `integer` | `int` |  |  | Blocked for Assembly. Allowed values: 1, 2 |
| `bass_kw` | `string` | `varchar` |  |  | Blocked for Assembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `filo` | `integer` | `int` |  |  | Fixed Location. Allowed values: 1, 2 |
| `filo_kw` | `string` | `varchar` |  |  | Fixed Location (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aoit` | `integer` | `int` |  |  | Allow Other Items. Allowed values: 1, 2 |
| `aoit_kw` | `string` | `varchar` |  |  | Allow Other Items (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prio` | `integer` | `int` |  |  | Inbound Priority |
| `proo` | `integer` | `int` |  |  | Outbound Priority |
| `tran` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Status Change |
| `cmes` | `string` | `varchar` |  |  | Remark. Max length: 3 |
| `itlo` | `integer` | `int` |  |  | Multi-Item Locations. Allowed values: 1, 2 |
| `itlo_kw` | `string` | `varchar` |  |  | Multi-Item Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lolo` | `integer` | `int` |  |  | Multi-Lot by Item Locations. Allowed values: 1, 2 |
| `lolo_kw` | `string` | `varchar` |  |  | Multi-Lot by Item Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `_full` | `integer` | `int` |  |  | Location Full. Allowed values: 1, 2 |
| `full_kw` | `string` | `varchar` |  |  | Location Full (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cadr` | `string` | `varchar` |  |  | Address. Max length: 9 |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `rntl` | `integer` | `int` |  |  | Rental. Allowed values: 1, 2, 3 |
| `rntl_kw` | `string` | `varchar` |  |  | Rental (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `txta` | `integer` | `int` |  |  | Text |
| `cwar_zone_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd310 Storage Zones |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `cmes_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd130 Remarks |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
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
