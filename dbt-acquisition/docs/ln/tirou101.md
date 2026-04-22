# tirou101

LN tirou101 Routing Codes by Item table, Generated 2026-04-10T19:41:49Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tirou101` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `mitm`, `opro` |
| **Column count** | 39 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `mitm` | `string` | `varchar` | `PK` | `not_null` | (required) Manufactured Item. Max length: 47 |
| `opro` | `string` | `varchar` | `PK` | `not_null` | (required) Routing. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `stor` | `integer` | `int` |  |  | Standard Routing. Allowed values: 1, 2 |
| `stor_kw` | `string` | `varchar` |  |  | Standard Routing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `strc` | `string` | `varchar` |  |  | Standard Routing Code. Max length: 6 |
| `woar` | `string` | `varchar` |  |  | Scheduling Area. Max length: 3 |
| `ruoq` | `float` | `float` |  |  | Up to Order Quantity |
| `runi` | `float` | `float` |  |  | Routing Unit |
| `rcal` | `float` | `float` |  |  | Calculated Prod. Rate |
| `bwce` | `string` | `varchar` |  |  | Calculated Bottleneck Work Center. Max length: 6 |
| `ract` | `float` | `float` |  |  | Corrected Prod. Rate |
| `bwca` | `string` | `varchar` |  |  | Corrected Bottleneck Work Center. Max length: 6 |
| `lcdr` | `timestamp` | `timestamp_ntz` |  |  | Last Calculation Date Rate |
| `mrau` | `integer` | `int` |  |  | Automatic Update. Allowed values: 1, 2 |
| `mrau_kw` | `string` | `varchar` |  |  | Automatic Update (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `unef` | `integer` | `int` |  |  | Exceptions Present. Allowed values: 1, 2 |
| `unef_kw` | `string` | `varchar` |  |  | Exceptions Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stcf` | `integer` | `int` |  |  | Standard Configuration. Allowed values: 1, 2 |
| `stcf_kw` | `string` | `varchar` |  |  | Standard Configuration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wkbt` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `wkbt_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Routing Text |
| `mitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tiipd001 Items - Production |
| `woar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirpt003 Scheduling Areas |
| `bwce_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `bwca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
