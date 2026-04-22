# tsmdm140

LN tsmdm140 Service Employees table, Generated 2026-04-10T19:42:37Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsmdm140` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `emno` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `emno` | `string` | `varchar` | `PK` | `not_null` | (required) Service Employee. Max length: 9 |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `ccar` | `string` | `varchar` |  |  | Service Car. Max length: 10 |
| `mopd` | `float` | `float` |  |  | Maximum Overtime per Day |
| `pgen` | `integer` | `int` |  |  | Pager / Phone Enabled. Allowed values: 1, 2 |
| `pgen_kw` | `string` | `varchar` |  |  | Pager / Phone Enabled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asmc` | `string` | `varchar` |  |  | Assignment. Max length: 3 |
| `nots__en_us` | `string` | `varchar` |  | `not_null` | (required) Notes - base language. Max length: 50 |
| `lsdt` | `timestamp` | `timestamp_ntz` |  |  | Last Transaction Date |
| `ucts` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `ucts_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frmn` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `ccar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm145 Service Cars |
| `asmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm185 Paging Assignments |
| `frmn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
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
