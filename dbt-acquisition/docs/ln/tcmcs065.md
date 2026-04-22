# tcmcs065

LN tcmcs065 Departments table, Generated 2026-04-10T19:41:13Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs065` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwoc` |
| **Column count** | 34 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwoc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `typd` | `integer` | `int` |  |  | Department Type. Allowed values: 1, 2, 3, 4, 5, 6, 8, 10, 15 |
| `typd_kw` | `string` | `varchar` |  |  | Department Type (keyword). Allowed values: tctypd.sls.office, tctypd.pur.office, tctypd.service.dept, tctypd.workcentre, tctypd.production.dept, tctypd.accounting, tctypd.shipping.office, tctypd.proj.mgt.office, tctypd.inv.man.dept |
| `comp` | `integer` | `int` |  |  | Financial Company |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `cadr` | `string` | `varchar` |  |  | Address. Max length: 9 |
| `city__en_us` | `string` | `varchar` |  | `not_null` | (required) City (Letterhead) - base language. Max length: 30 |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `emno` | `string` | `varchar` |  |  | Manager. Max length: 9 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `clrt` | `string` | `varchar` |  |  | Labor Rate Code. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `dstp` | `integer` | `int` |  |  | Department Subtype. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 75, 99 |
| `dstp_kw` | `string` | `varchar` |  |  | Department Subtype (keyword). Allowed values: tcdpt.styp.asm.cell, tcdpt.styp.asl.buffer, tcdpt.styp.asl.station, tcdpt.styp.work.cell, tcdpt.styp.repair.cell, tcdpt.styp.js.wc, tcdpt.styp.js.subwc, tcdpt.styp.js.subc, tcdpt.styp.cost, tcdpt.styp.calc.office, tcdpt.styp.transformation, tcdpt.styp.not.applicable |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwoc_eunt` | `string` | `varchar` |  |  | CC: Enterprise Unit of Department. Max length: 6 |
| `eunt_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Enterprise Unit |
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
