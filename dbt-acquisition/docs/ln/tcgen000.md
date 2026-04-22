# tcgen000

LN tcgen000 Generic Parameters table, Generated 2026-04-10T19:41:07Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcgen000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Effective Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `awdt` | `integer` | `int` |  |  | Active WS for Transportation Docs (WDT). Allowed values: 1, 2 |
| `awdt_kw` | `string` | `varchar` |  |  | Active WS for Transportation Docs (WDT) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wdto__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WDT Outbox - base language. Max length: 132 |
| `wdti__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WDT Inbox (Registered Responses) - base language. Max length: 132 |
| `wdte__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WDT Inbox (Unregistered Responses) - base language. Max length: 132 |
| `wdtr__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WDT Archive (Registered Responses) - base language. Max length: 132 |
| `awfa` | `integer` | `int` |  |  | Active WS for Sales Invoices (WFA). Allowed values: 1, 2 |
| `awfa_kw` | `string` | `varchar` |  |  | Active WS for Sales Invoices (WFA) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wfao__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WFA Outbox - base language. Max length: 132 |
| `wfai__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WFA Inbox (Registered Responses) - base language. Max length: 132 |
| `wfae__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WFA Inbox (Unregistered Responses) - base language. Max length: 132 |
| `wfar__en_us` | `string` | `varchar` |  | `not_null` | (required) Path for WFA Archive (Registered Responses) - base language. Max length: 132 |
| `time` | `integer` | `int` |  |  | Seconds for Timeout |
| `crpd` | `integer` | `int` |  |  | Retention Period [Days] |
| `clcd` | `timestamp` | `timestamp_ntz` |  |  | Latest Cleanup Date |
| `cvfd` | `timestamp` | `timestamp_ntz` |  |  | Valid From Date |
| `dcur` | `string` | `varchar` |  |  | Data Warehouse Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cpie` | `string` | `varchar` |  |  | Piece. Max length: 3 |
| `dcmp` | `integer` | `int` |  |  | Data Warehouse Setup in Company |
| `urtp` | `integer` | `int` |  |  | Use Specific Exchange Rate Type. Allowed values: 1, 2 |
| `urtp_kw` | `string` | `varchar` |  |  | Use Specific Exchange Rate Type (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `udwc` | `integer` | `int` |  |  | Use Data Warehouse Currency. Allowed values: 1, 2 |
| `udwc_kw` | `string` | `varchar` |  |  | Use Data Warehouse Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `urfc` | `integer` | `int` |  |  | Use Reference Currency. Allowed values: 1, 2 |
| `urfc_kw` | `string` | `varchar` |  |  | Use Reference Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uslc` | `integer` | `int` |  |  | Use Local Currency. Allowed values: 1, 2 |
| `uslc_kw` | `string` | `varchar` |  |  | Use Local Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `urc1` | `integer` | `int` |  |  | Use Reporting Currency 1. Allowed values: 1, 2 |
| `urc1_kw` | `string` | `varchar` |  |  | Use Reporting Currency 1 (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `urc2` | `integer` | `int` |  |  | Use Reporting Currency 2. Allowed values: 1, 2 |
| `urc2_kw` | `string` | `varchar` |  |  | Use Reporting Currency 2 (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ustc` | `integer` | `int` |  |  | Use Transaction Currency. Allowed values: 1, 2 |
| `ustc_kw` | `string` | `varchar` |  |  | Use Transaction Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dcur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cpie_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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
