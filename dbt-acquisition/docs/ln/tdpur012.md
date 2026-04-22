# tdpur012

LN tdpur012 Purchase Offices table, Generated 2026-04-10T19:41:17Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur012` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cofc` |
| **Column count** | 53 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Office. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `ngpo` | `string` | `varchar` |  |  | Purchase Order Group. Max length: 3 |
| `sepo` | `string` | `varchar` |  |  | Purchase Order Series. Max length: 8 |
| `seps` | `string` | `varchar` |  |  | Schedule Series. Max length: 8 |
| `ngsp` | `string` | `varchar` |  |  | Number Group for Services Procurement. Max length: 3 |
| `sspo` | `string` | `varchar` |  |  | Series for Subcontracting Procurement Orders. Max length: 8 |
| `seqo` | `string` | `varchar` |  |  | Series for Equipment Procurement Orders. Max length: 8 |
| `safp` | `string` | `varchar` |  |  | Series for Applications for Payment. Max length: 8 |
| `sequ` | `string` | `varchar` |  |  | Series for Equipment Utilizations. Max length: 8 |
| `ngpq` | `string` | `varchar` |  |  | RFQ Group. Max length: 3 |
| `sepq` | `string` | `varchar` |  |  | RFQ Series. Max length: 8 |
| `ngpc` | `string` | `varchar` |  |  | Contract Group. Max length: 3 |
| `sepc` | `string` | `varchar` |  |  | Contract Series. Max length: 8 |
| `ngrl` | `string` | `varchar` |  |  | Release Number Group. Max length: 3 |
| `serl` | `string` | `varchar` |  |  | Release Series. Max length: 8 |
| `ngpr` | `string` | `varchar` |  |  | Number Group Requisitions. Max length: 3 |
| `sepr` | `string` | `varchar` |  |  | Series Purchase Requisition. Max length: 8 |
| `ngco` | `string` | `varchar` |  |  | Number Group for Call-offs. Max length: 3 |
| `scid` | `string` | `varchar` |  |  | Call-off Series. Max length: 8 |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `ngpo_sepo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpo_seps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngsp_sspo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsp_seqo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsp_safp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsp_sequ_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpq_sepq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpc_sepc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngrl_serl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngrl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngpr_sepr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngco_scid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
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
