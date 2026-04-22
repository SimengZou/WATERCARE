# tpppc600

LN tpppc600 Overhead Cost Transactions table, Generated 2026-04-10T19:42:17Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `user`, `cprj`, `base`, `cspa`, `cact`, `cotp`, `coob`, `sern`, `ohcs` |
| **Column count** | 51 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `user` | `string` | `varchar` | `PK` | `not_null` | (required) User. Max length: 16 |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `base` | `string` | `varchar` | `PK` | `not_null` | (required) Base. Max length: 8 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `cotp` | `integer` | `int` | `PK` | `not_null` | (required) Cost Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `cotp_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tppdm.cotp.tasks, tppdm.cotp.materials, tppdm.cotp.equipment, tppdm.cotp.subcontracting, tppdm.cotp.indirect, tppdm.cotp.overhead |
| `coob` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Object. Max length: 47 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `ohcs` | `integer` | `int` | `PK` | `not_null` | (required) Overhead Cost Sequence |
| `tcob` | `string` | `varchar` |  |  | Target Cost Object. Max length: 8 |
| `cccp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `vrsn` | `integer` | `int` |  |  | Version |
| `btyp` | `integer` | `int` |  |  | Base Type. Allowed values: 1, 2 |
| `btyp_kw` | `string` | `varchar` |  |  | Base Type (keyword). Allowed values: tppdm.btyp.amount, tppdm.btyp.direct.labor.h |
| `amnt` | `float` | `float` |  |  | Base Value Amount |
| `hour` | `float` | `float` |  |  | Base Value Hours |
| `unit` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `perc` | `float` | `float` |  |  | Overhead Percentage |
| `rate` | `float` | `float` |  |  | Overhead Rate |
| `levl` | `integer` | `int` |  |  | Level |
| `tbap` | `float` | `float` |  |  | Overhead to be Applied |
| `calc` | `float` | `float` |  |  | Calculated Overhead |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `bamt` | `float` | `float` |  |  | Billing Base Value Amount |
| `bprc` | `float` | `float` |  |  | Billing Percentage |
| `brte` | `float` | `float` |  |  | Billing Rate |
| `btba` | `float` | `float` |  |  | Billing Amount to be Applied |
| `bclc` | `float` | `float` |  |  | Billing Calculated Overhead |
| `bcur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ohst` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4 |
| `ohst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tpppc.ohst.open, tpppc.ohst.not.able.apply, tpppc.ohst.applied, tpppc.ohst.not.applied |
| `appl` | `integer` | `int` |  |  | Apply. Allowed values: 1, 2 |
| `appl_kw` | `string` | `varchar` |  |  | Apply (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ctdt` | `timestamp` | `timestamp_ntz` |  |  | Calculation Transaction Date |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Date |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Calculation Date |
| `year` | `integer` | `int` |  |  | Year |
| `base_vrsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm200 Overhead Application Base |
| `tcob_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm042 Standard Overhead |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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
