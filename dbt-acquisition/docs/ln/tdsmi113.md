# tdsmi113

LN tdsmi113 Items by Opportunity table, Generated 2026-04-10T19:41:27Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsmi113` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `opty`, `pono` |
| **Column count** | 45 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `opty` | `string` | `varchar` | `PK` | `not_null` | (required) Opportunity. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `seli` | `integer` | `int` |  |  | Select Item. Allowed values: 1, 2 |
| `seli_kw` | `string` | `varchar` |  |  | Select Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `qoor` | `float` | `float` |  |  | Ordered Quantity |
| `cuqs` | `string` | `varchar` |  |  | Order Unit. Max length: 3 |
| `cvqs` | `float` | `float` |  |  | Conversion Factor |
| `amta` | `float` | `float` |  |  | Amount |
| `prid` | `string` | `varchar` |  |  | Price Information. Max length: 22 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `opty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsmi110 Opportunities |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa001 Item - Sales |
| `cuqs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `amta_lclc` | `float` | `float` |  |  | CC: Net Amount in Local Currency |
| `amta_rpc1` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 1 |
| `amta_rpc2` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 2 |
| `amta_rfrc` | `float` | `float` |  |  | CC: Net Amount in Reference Currency |
| `amta_dtwc` | `float` | `float` |  |  | CC: Net Amount in Data Warehouse Currency |
| `qoor_invu` | `float` | `float` |  |  | CC: Ordered Quantity in Inventory Unit |
| `qoor_buar` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Area |
| `qoor_buln` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Length |
| `qoor_bupc` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Piece |
| `qoor_butm` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Time |
| `qoor_buvl` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Volume |
| `qoor_buwg` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Weight |
| `amtg_trnc` | `float` | `float` |  |  | CC: Gross Amount in Transaction Currency |
| `amtg_lclc` | `float` | `float` |  |  | CC: Gross Amount in Local Currency |
| `amtg_rpc1` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 1 |
| `amtg_rpc2` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 2 |
| `amtg_rfrc` | `float` | `float` |  |  | CC: Gross Amount in Reference Currency |
| `amtg_dtwc` | `float` | `float` |  |  | CC: Gross Amount in Data Warehouse Currency |
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
