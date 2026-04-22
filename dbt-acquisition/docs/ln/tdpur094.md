# tdpur094

LN tdpur094 Purchase Order Types table, Generated 2026-04-10T19:41:17Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur094` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `potp` |
| **Column count** | 51 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `potp` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order Type. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `drct` | `integer` | `int` |  |  | Direct Delivery. Allowed values: 1, 2 |
| `drct_kw` | `string` | `varchar` |  |  | Direct Delivery (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `reto` | `integer` | `int` |  |  | Return Order. Allowed values: 1, 2, 3 |
| `reto_kw` | `string` | `varchar` |  |  | Return Order (keyword). Allowed values: tdpur.reto.yes, tdpur.reto.no, tdpur.reto.rejectgoods |
| `coun` | `integer` | `int` |  |  | Collect Order. Allowed values: 1, 2 |
| `coun_kw` | `string` | `varchar` |  |  | Collect Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sund` | `integer` | `int` |  |  | Cost Order. Allowed values: 1, 2 |
| `sund_kw` | `string` | `varchar` |  |  | Cost Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnsp` | `integer` | `int` |  |  | Consignment Payment. Allowed values: 1, 2 |
| `cnsp_kw` | `string` | `varchar` |  |  | Consignment Payment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnsr` | `integer` | `int` |  |  | Consignment Replenish. Allowed values: 1, 2 |
| `cnsr_kw` | `string` | `varchar` |  |  | Consignment Replenish (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `subc` | `integer` | `int` |  |  | Subcontracting Order. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracting Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfnm` | `integer` | `int` |  |  | Customer Furnished Materials. Allowed values: 1, 2 |
| `cfnm_kw` | `string` | `varchar` |  |  | Customer Furnished Materials (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wrhp` | `string` | `varchar` |  |  | Warehousing Order Type. Max length: 3 |
| `slcp` | `integer` | `int` |  |  | Ship Lines Complete. Allowed values: 1, 2 |
| `slcp_kw` | `string` | `varchar` |  |  | Ship Lines Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cbor` | `integer` | `int` |  |  | Combine Open Backorders. Allowed values: 1, 2 |
| `cbor_kw` | `string` | `varchar` |  |  | Combine Open Backorders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngpo` | `string` | `varchar` |  |  | Purchase Order Group. Max length: 3 |
| `sepo` | `string` | `varchar` |  |  | Purchase Order Series. Max length: 8 |
| `ngpq` | `string` | `varchar` |  |  | RFQ Group. Max length: 3 |
| `sepq` | `string` | `varchar` |  |  | RFQ Series. Max length: 8 |
| `ngpc` | `string` | `varchar` |  |  | Contract Group. Max length: 3 |
| `sepc` | `string` | `varchar` |  |  | Contract Series. Max length: 8 |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `proc` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `pmsk` | `string` | `varchar` |  |  | Obsolete. Max length: 20 |
| `ngpo_sepo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpq_sepq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpc_sepc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
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
