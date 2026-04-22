# tppin011

LN tppin011 Settled Advance Payments table, Generated 2026-04-10T19:42:04Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin011` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `sern`, `cprj`, `ofbp`, `serl` |
| **Column count** | 52 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Advance Payment |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ofbp` | `string` | `varchar` | `PK` | `not_null` | (required) Sold-to Business Partner. Max length: 9 |
| `serl` | `integer` | `int` | `PK` | `not_null` | (required) Settlement |
| `cnpr` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `amnt` | `float` | `float` |  |  | Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `pamt` | `float` | `float` |  |  | Amount Paid |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `idln` | `integer` | `int` |  |  | Invoice Document Line |
| `iseq` | `integer` | `int` |  |  | Composed Invoice |
| `adva` | `integer` | `int` |  |  | Credit Advance |
| `nins` | `integer` | `int` |  |  | Installment |
| `dlvr` | `integer` | `int` |  |  | Deliverable |
| `schd` | `integer` | `int` |  |  | Schedule |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Progress Date |
| `cono_cnln_sern_cprj_ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin010 Advance Payments |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cnpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `amnt_refc` | `float` | `float` |  |  | CC: Settled Advance Amount in Reference Currency |
| `amnt_cntc` | `float` | `float` |  |  | CC: Settled Advance Amount in Contract Currency |
| `amnt_prjc` | `float` | `float` |  |  | CC: Settled Advance Amount in Project Currency |
| `amnt_homc` | `float` | `float` |  |  | CC: Settled Advance Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Settled Advance Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Settled Advance Amount in Reporting Currency 2 |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Settled Advance Amount in Data Warehouse Currency |
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
