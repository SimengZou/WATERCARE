# tsctm480

LN tsctm480 Contract Cost Coverage - Overview table, Generated 2026-04-10T19:42:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm480` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csco`, `cchn`, `cfln`, `seqn` |
| **Column count** | 41 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csco` | `string` | `varchar` | `PK` | `not_null` | (required) Service Contract. Max length: 9 |
| `cchn` | `integer` | `int` | `PK` | `not_null` | (required) Origin Contract Change |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Cost Line |
| `ivsq` | `integer` | `int` |  |  | Invoice Line Sequence |
| `acco_1` | `float` | `float` |  |  | Actual Cost |
| `acco_2` | `float` | `float` |  |  | Actual Cost |
| `acco_3` | `float` | `float` |  |  | Actual Cost |
| `codt` | `timestamp` | `timestamp_ntz` |  |  | Date Spent Costs |
| `plyr` | `integer` | `int` |  |  | Planned Year |
| `plpr` | `integer` | `int` |  |  | Planned Period |
| `ortp` | `integer` | `int` |  |  | Order Type. Allowed values: 5, 10, 15, 20, 25 |
| `ortp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tsctm.ortp.serv, tsctm.ortp.maint, tsctm.ortp.call, tsctm.ortp.claim, tsctm.ortp.quote |
| `orno` | `string` | `varchar` |  |  | Order Number. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Order Line Number |
| `cvln` | `integer` | `int` |  |  | Coverage Line Number |
| `cotp` | `integer` | `int` |  |  | Cost Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 45, 47, 50, 51, 60 |
| `cotp_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tsmdm.cotp.material, tsmdm.cotp.labor, tsmdm.cotp.tool, tsmdm.cotp.travel, tsmdm.cotp.subcon, tsmdm.cotp.helpdesk, tsmdm.cotp.other, tsmdm.cotp.order, tsmdm.cotp.activity, tsmdm.cotp.freight, tsmdm.cotp.quotinv, tsmdm.cotp.rental |
| `poyr` | `integer` | `int` |  |  | Posted Fiscal Year |
| `popr` | `integer` | `int` |  |  | Posted Fiscal Period |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 5, 10, 15, 20 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tsctm.csst.free, tsctm.csst.costed, tsctm.csst.posted, tsctm.csst.approved |
| `ircl` | `integer` | `int` |  |  | Include in Revenue Calculation. Allowed values: 1, 2 |
| `ircl_kw` | `string` | `varchar` |  |  | Include in Revenue Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rchn` | `integer` | `int` |  |  | Change Number of Contract Period |
| `csco_cchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_rchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `acco_cntc` | `float` | `float` |  |  | CC: Actual Cost Amount in Contract Currency |
| `acco_refc` | `float` | `float` |  |  | CC: Actual Cost Amount in Reference Currency |
| `acco_dwhc` | `float` | `float` |  |  | CC: Actual Cost Amount in Data Warehouse Currency |
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
