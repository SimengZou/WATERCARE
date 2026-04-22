# tipcs030

LN tipcs030 Project Details table, Generated 2026-04-10T19:41:48Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tipcs030` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj` |
| **Column count** | 62 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `psts` | `integer` | `int` |  |  | Project Status. Allowed values: 1, 2, 3, 4, 5, 6, 7, 9 |
| `psts_kw` | `string` | `varchar` |  |  | Project Status (keyword). Allowed values: tcpsts.free, tcpsts.simulation, tcpsts.active, tcpsts.cancelled, tcpsts.finished, tcpsts.closed, tcpsts.to.be.closed, tcpsts.archived |
| `psta` | `string` | `varchar` |  |  | Project Stage. Max length: 3 |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `intp` | `integer` | `int` |  |  | Investment Project. Allowed values: 1, 2 |
| `intp_kw` | `string` | `varchar` |  |  | Investment Project (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `erev` | `integer` | `int` |  |  | Revenue Expected. Allowed values: 1, 2 |
| `erev_kw` | `string` | `varchar` |  |  | Revenue Expected (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bkcs` | `integer` | `int` |  |  | Post Project Transactions on Current Date. Allowed values: 1, 2 |
| `bkcs_kw` | `string` | `varchar` |  |  | Post Project Transactions on Current Date (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bkdt` | `timestamp` | `timestamp_ntz` |  |  | Posting Date Cost/Turnover Sales |
| `tinv_1` | `float` | `float` |  |  | Invoiced Amount |
| `tinv_2` | `float` | `float` |  |  | Invoiced Amount |
| `tinv_3` | `float` | `float` |  |  | Invoiced Amount |
| `ascp` | `float` | `float` |  |  | Spent Critical Capacity |
| `ecpa` | `float` | `float` |  |  | Estimated Capacity (Aggregated) |
| `ecpr` | `float` | `float` |  |  | Estimated Capacity (Rough) |
| `cbdg` | `string` | `varchar` |  |  | Budget. Max length: 9 |
| `plcd` | `integer` | `int` |  |  | Planning Method. Allowed values: 1, 2 |
| `plcd_kw` | `string` | `varchar` |  |  | Planning Method (keyword). Allowed values: tcplcd.forward, tcplcd.backward |
| `defc` | `integer` | `int` |  |  | Definitively Closed. Allowed values: 1, 2 |
| `defc_kw` | `string` | `varchar` |  |  | Definitively Closed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Actual Closing date |
| `mib4` | `integer` | `int` |  |  | Migrated from Baan IV. Allowed values: 0, 1, 2 |
| `mib4_kw` | `string` | `varchar` |  |  | Migrated from Baan IV (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `soco` | `float` | `float` |  |  | Stage of Completion |
| `pocm` | `integer` | `int` |  |  | Percentage of Completion Calculation Method. Allowed values: 1, 2, 3, 4 |
| `pocm_kw` | `string` | `varchar` |  |  | Percentage of Completion Calculation Method (keyword). Allowed values: tipcs.pocm.none, tipcs.pocm.cost, tipcs.pocm.hours, tipcs.pocm.manual |
| `intc` | `integer` | `int` |  |  | Post Interim COS and Revenues by Cost Component. Allowed values: 1, 2 |
| `intc_kw` | `string` | `varchar` |  |  | Post Interim COS and Revenues by Cost Component (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `incp` | `string` | `varchar` |  |  | General Cost Component for Interim Postings. Max length: 8 |
| `rrtp` | `integer` | `int` |  |  | Revenue Recognition by Transaction Price. Allowed values: 1, 2 |
| `rrtp_kw` | `string` | `varchar` |  |  | Revenue Recognition by Transaction Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `otac` | `integer` | `int` |  |  | Over Time Accounting. Allowed values: 1, 2 |
| `otac_kw` | `string` | `varchar` |  |  | Over Time Accounting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Actualization Date |
| `crfc` | `integer` | `int` |  |  | COS and Revenues Restricted to Financial Company of PCS Project. Allowed values: 1, 2 |
| `crfc_kw` | `string` | `varchar` |  |  | COS and Revenues Restricted to Financial Company of PCS Project (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dstr` | `integer` | `int` |  |  | COS Distribution based on. Allowed values: 10, 20, 30 |
| `dstr_kw` | `string` | `varchar` |  |  | COS Distribution based on (keyword). Allowed values: tipcs.distr.not.applicable, tipcs.distr.manual, tipcs.distr.calculated |
| `tpsr` | `integer` | `int` |  |  | Transfer between PCS Projects considered as Selling Relation. Allowed values: 1, 2 |
| `tpsr_kw` | `string` | `varchar` |  |  | Transfer between PCS Projects considered as Selling Relation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aman` | `float` | `float` |  |  | Actual Labor Hours |
| `amch` | `float` | `float` |  |  | Actual Machine Hours |
| `ncac` | `integer` | `int` |  |  | Net Change Actual Costs. Allowed values: 1, 2 |
| `ncac_kw` | `string` | `varchar` |  |  | Net Change Actual Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tipcs020 General Project Data |
| `psta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tipcs024 Project Stages |
| `cbdg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tipcs020 General Project Data |
| `incp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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
