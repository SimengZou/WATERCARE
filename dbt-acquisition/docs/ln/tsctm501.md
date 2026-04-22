# tsctm501

LN tsctm501 Warranty Transactions table, Generated 2026-04-10T19:42:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm501` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `wrty`, `item`, `sern`, `trtm`, `seqn` |
| **Column count** | 56 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `wrty` | `string` | `varchar` | `PK` | `not_null` | (required) Warranty. Max length: 16 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `sern` | `string` | `varchar` | `PK` | `not_null` | (required) Serial Number. Max length: 30 |
| `trtm` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Time |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cctp` | `string` | `varchar` |  |  | Coverage Type. Max length: 3 |
| `cotp` | `integer` | `int` |  |  | Term Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45 |
| `cotp_kw` | `string` | `varchar` |  |  | Term Type (keyword). Allowed values: tsctm.tmtp.material, tsctm.tmtp.labor, tsctm.tmtp.tool, tsctm.tmtp.travel, tsctm.tmtp.subcon, tsctm.tmtp.helpdesk, tsctm.tmtp.other, tsctm.tmtp.uptime, tsctm.tmtp.all |
| `ortp` | `integer` | `int` |  |  | Order Type. Allowed values: 5, 10, 15, 20, 25 |
| `ortp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tsctm.ortp.serv, tsctm.ortp.maint, tsctm.ortp.call, tsctm.ortp.claim, tsctm.ortp.quote |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Order Line |
| `cvln` | `integer` | `int` |  |  | Order Detail Line |
| `tcst` | `integer` | `int` |  |  | Cost Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 45, 47, 50, 51, 60 |
| `tcst_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tsmdm.cotp.material, tsmdm.cotp.labor, tsmdm.cotp.tool, tsmdm.cotp.travel, tsmdm.cotp.subcon, tsmdm.cotp.helpdesk, tsmdm.cotp.other, tsmdm.cotp.order, tsmdm.cotp.activity, tsmdm.cotp.freight, tsmdm.cotp.quotinv, tsmdm.cotp.rental |
| `wrtp` | `integer` | `int` |  |  | Warranty Type. Allowed values: 5, 10, 15 |
| `wrtp_kw` | `string` | `varchar` |  |  | Warranty Type (keyword). Allowed values: tstdm.wrtp.serial, tstdm.wrtp.general, tstdm.wrtp.no.warranty |
| `cstp` | `string` | `varchar` |  |  | Service Type. Max length: 3 |
| `cwoc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `litm` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `lser` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `spco_1` | `float` | `float` |  |  | Spent Costs |
| `spco_2` | `float` | `float` |  |  | Spent Costs |
| `spco_3` | `float` | `float` |  |  | Spent Costs |
| `spsa` | `float` | `float` |  |  | Spent Sales |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `cstp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm030 Service Types |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `litm_lser_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `litm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `spco_trnc` | `float` | `float` |  |  | CC: Spent Cost Amount in Contract Currency |
| `spco_refc` | `float` | `float` |  |  | CC: Spent Cost Amount in Reference Currency |
| `spco_dwhc` | `float` | `float` |  |  | CC: Spent Cost Amount in Data Warehouse Currency |
| `spsa_homc` | `float` | `float` |  |  | CC: Spent Sales Amount in Local Currency |
| `spsa_rpc1` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 1 |
| `spsa_rpc2` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 2 |
| `spsa_refc` | `float` | `float` |  |  | CC: Spent Sales Amount in Reference Currency |
| `spsa_dwhc` | `float` | `float` |  |  | CC: Spent Sales Amount in Data Warehouse Currency |
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
