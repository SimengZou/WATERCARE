# tppin050

LN tppin050 Transferred Unit Rate Invoice Lines table, Generated 2026-04-10T19:42:05Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin050` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cspa`, `cpla`, `cact`, `sern` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Serial Number |
| `pgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `quan` | `float` | `float` |  |  | Transferred Quantity |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `amos` | `float` | `float` |  |  | Transferred Amount |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `amos_refc` | `float` | `float` |  |  | CC: Transferred Amount in Reference Currency |
| `amos_cntc` | `float` | `float` |  |  | CC: Transferred Amount in Contract Currency |
| `amos_prjc` | `float` | `float` |  |  | CC: Transferred Amount in Project Currency |
| `amos_homc` | `float` | `float` |  |  | CC: Transferred Amount in Local Currency |
| `amos_rpc1` | `float` | `float` |  |  | CC: Transferred Amount in Reporting Currency 1 |
| `amos_rpc2` | `float` | `float` |  |  | CC: Transferred Amount in Reporting Currency 2 |
| `amos_dwhc` | `float` | `float` |  |  | CC: Transferred Amount in Data Warehouse Currency |
| `quan_buar` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Area |
| `quan_buvl` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Volume |
| `quan_buln` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Length |
| `quan_bupc` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Piece |
| `quan_buwg` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Weight |
| `quan_butm` | `float` | `float` |  |  | CC: Transferred Quantity in Base Unit Time |
| `quan_hour` | `float` | `float` |  |  | CC: Transferred Quantity in Hours |
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
