# qmptc220

LN qmptc220 Storage Inspections table, Generated 2022-06-15T01:21:06Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc220` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `effn`, `revi`, `seqn` |
| **Column count** | 69 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `effn` | `integer` | `int` | `PK` | `not_null` | (required) Effectivity Unit |
| `revi` | `string` | `varchar` | `PK` | `not_null` | (required) E-Item Revision. Max length: 6 |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `oqua` | `float` | `float` |  |  | Quantity Ordered |
| `raqa` | `float` | `float` |  |  | Recommended Quantity Accepted |
| `rdqa` | `float` | `float` |  |  | Recommended Quantity Destroyed |
| `rrqa` | `float` | `float` |  |  | Recommended Quantity Rejected |
| `aaqa` | `float` | `float` |  |  | Actual Quantity Accepted |
| `arqa` | `float` | `float` |  |  | Actual Quantity Rejected |
| `cdis` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `rsta` | `integer` | `int` |  |  | Inspection Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `rsta_kw` | `string` | `varchar` |  |  | Inspection Status (keyword). Allowed values: qmptc.rsta.free, qmptc.rsta.active, qmptc.rsta.completed, qmptc.rsta.processed, qmptc.rsta.closed, qmptc.rsta.cancelled |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `pdat` | `timestamp` | `timestamp_ntz` |  |  | Processing Date |
| `gncm` | `integer` | `int` |  |  | Non-Conforming Material Report. Allowed values: 1, 2 |
| `gncm_kw` | `string` | `varchar` |  |  | Non-Conforming Material Report (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Activation Date |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Closing Date |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Number |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `oqua_buar` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Area |
| `oqua_buln` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Length |
| `oqua_bupc` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Piece |
| `oqua_butm` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Time |
| `oqua_buvl` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Volume |
| `oqua_buwg` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Weight |
| `raqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Area |
| `raqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Length |
| `raqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Piece |
| `raqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Time |
| `raqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Volume |
| `raqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Weight |
| `rrqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Area |
| `rrqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Length |
| `rrqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Piece |
| `rrqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Time |
| `rrqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Volume |
| `rrqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Weight |
| `rdqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Area |
| `rdqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Length |
| `rdqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Piece |
| `rdqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Time |
| `rdqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Volume |
| `rdqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Weight |
| `aaqa_buar` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Area |
| `aaqa_buln` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Length |
| `aaqa_bupc` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Piece |
| `aaqa_butm` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Time |
| `aaqa_buvl` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Volume |
| `aaqa_buwg` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Weight |
| `arqa_buar` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Area |
| `arqa_buln` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Length |
| `arqa_bupc` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Piece |
| `arqa_butm` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Time |
| `arqa_buvl` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Volume |
| `arqa_buwg` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Weight |
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
