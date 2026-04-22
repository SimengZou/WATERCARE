# txptc902

LN txptc902 Project Risks table, Generated 2026-04-10T19:42:42Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_txptc902` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `prsk` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `prsk` | `string` | `varchar` | `PK` | `not_null` | (required) Project Risk. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `ersk` | `string` | `varchar` |  |  | d/EPM Risk. Max length: 20 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 100 |
| `text` | `integer` | `int` |  |  | Risk Text |
| `creq` | `integer` | `int` |  |  | Change Request. Allowed values: 1, 2 |
| `creq_kw` | `string` | `varchar` |  |  | Change Request (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vreq` | `integer` | `int` |  |  | Variation Request. Allowed values: 1, 2 |
| `vreq_kw` | `string` | `varchar` |  |  | Variation Request (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcat` | `string` | `varchar` |  |  | Risk Category. Max length: 50 |
| `tdat` | `date` | `date` |  |  | Target Closure Date |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `crno` | `string` | `varchar` |  |  | Change Request - CR. Max length: 9 |
| `crpo` | `integer` | `int` |  |  | CR Line |
| `vrno` | `string` | `varchar` |  |  | Change Request - VR. Max length: 9 |
| `vrpo` | `integer` | `int` |  |  | VR Line |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
