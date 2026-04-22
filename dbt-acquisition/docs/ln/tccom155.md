# tccom155

LN tccom155 Business Partner - Satellites table, Generated 2026-04-10T19:41:04Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom155` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `bpid`, `cofc`, `kcod`, `code` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `kcod` | `integer` | `int` | `PK` | `not_null` | (required) Kind of Code. Allowed values: 5, 10, 15, 20, 25, 30, 255 |
| `kcod_kw` | `string` | `varchar` |  |  | Kind of Code (keyword). Allowed values: tcmcs.kcod.small.business, tcmcs.kcod.minority.owned, tcmcs.kcod.parent.sites, tcmcs.kcod.nat.of.sup, tcmcs.kcod.ser.item.grp, tcmcs.kcod.item.group, tcmcs.kcod.not.appl |
| `code` | `string` | `varchar` | `PK` | `not_null` | (required) Code. Max length: 12 |
| `cert__en_us` | `string` | `varchar` |  | `not_null` | (required) Certificate Number - base language. Max length: 30 |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
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
