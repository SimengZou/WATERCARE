# tsctm400

LN tsctm400 Contract Installments table, Generated 2026-04-10T19:42:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm400` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csco`, `inst` |
| **Column count** | 71 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csco` | `string` | `varchar` | `PK` | `not_null` | (required) Service Contract. Max length: 9 |
| `inst` | `integer` | `int` | `PK` | `not_null` | (required) Installment Number |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `stin` | `integer` | `int` |  |  | Installment Status. Allowed values: 5, 10, 15, 20, 25 |
| `stin_kw` | `string` | `varchar` |  |  | Installment Status (keyword). Allowed values: tsctm.stin.free, tsctm.stin.accepted, tsctm.stin.transferred, tsctm.stin.posted, tsctm.stin.canceled |
| `pidt` | `timestamp` | `timestamp_ntz` |  |  | Planned Invoice Date |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `isst` | `integer` | `int` |  |  | Installment Type. Allowed values: 5, 10, 15, 20, 25 |
| `isst_kw` | `string` | `varchar` |  |  | Installment Type (keyword). Allowed values: tsctm.isst.period, tsctm.isst.penalty, tsctm.isst.closure, tsctm.isst.down.time, tsctm.isst.manual |
| `amnt` | `float` | `float` |  |  | Installment Amount |
| `disc` | `float` | `float` |  |  | Discount Percentage |
| `dimt` | `float` | `float` |  |  | Discount Amount |
| `ntmt` | `float` | `float` |  |  | Net Installment Amount |
| `icmt` | `float` | `float` |  |  | Incidental Change Amount |
| `inmt` | `float` | `float` |  |  | Indexation Amount |
| `txmt` | `float` | `float` |  |  | Tax Amount |
| `fcpc` | `float` | `float` |  |  | Forecast Cost Percentage |
| `fcmt_1` | `float` | `float` |  |  | Forecast Cost Amount |
| `fcmt_2` | `float` | `float` |  |  | Forecast Cost Amount |
| `fcmt_3` | `float` | `float` |  |  | Forecast Cost Amount |
| `rahc_1` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `rahc_2` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `rahc_3` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `ratc_1` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratc_2` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratc_3` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratf_1` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratf_2` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratf_3` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `nrpf` | `integer` | `int` |  |  | Number of Periods for Financial Spreading |
| `indt` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `icmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Number |
| `fyer` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Posting Date |
| `istd` | `date` | `date` |  |  | Start Date |
| `cchn` | `integer` | `int` |  |  | Contract Change Number |
| `term` | `integer` | `int` |  |  | Term ID |
| `cfln` | `integer` | `int` |  |  | Configuration Line |
| `txta` | `integer` | `int` |  |  | Installment Text |
| `csco_cchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amnt_homc` | `float` | `float` |  |  | CC: Installment Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Installment Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Installment Amount in Reporting Currency 2 |
| `amnt_refc` | `float` | `float` |  |  | CC: Installment Amount in Reference Currency |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Installment Amount in Data Warehouse Currency |
| `dimt_homc` | `float` | `float` |  |  | CC: Discount Amount in Local Currency |
| `dimt_rpc1` | `float` | `float` |  |  | CC: Discount Amount in Reporting Currency 1 |
| `dimt_rpc2` | `float` | `float` |  |  | CC: Discount Amount in Reporting Currency 2 |
| `dimt_refc` | `float` | `float` |  |  | CC: Discount Amount in Reference Currency |
| `dimt_dwhc` | `float` | `float` |  |  | CC: Discount Amount in Data Warehouse Currency |
| `rahc_cntc` | `float` | `float` |  |  | CC: Revenue Amount in Contract Currency |
| `rahc_refc` | `float` | `float` |  |  | CC: Revenue Amount in Reference Currency |
| `rahc_dwhc` | `float` | `float` |  |  | CC: Revenue Amount in Data Warehouse Currency |
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
