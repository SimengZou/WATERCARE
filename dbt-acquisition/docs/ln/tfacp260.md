# tfacp260

LN tfacp260 Approval Error Log table, Generated 2026-04-10T19:41:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp260` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ityp`, `idoc`, `loco`, `otyp`, `orno`, `pono`, `sqnb` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ityp` | `string` | `varchar` | `PK` | `not_null` | (required) Obsolete. Max length: 3 |
| `idoc` | `integer` | `int` | `PK` | `not_null` | (required) Obsolete |
| `loco` | `integer` | `int` | `PK` | `not_null` | (required) Obsolete |
| `otyp` | `integer` | `int` | `PK` | `not_null` | (required) Obsolete. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `otyp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tfacp.otyp.purchase, tfacp.otyp.sales, tfacp.otyp.trans, tfacp.otyp.trans.man, tfacp.otyp.trans.wip, tfacp.otyp.freight, tfacp.otyp.commissions, tfacp.otyp.project, tfacp.otyp.project.man, tfacp.otyp.ent.planning, tfacp.otyp.assembly, tfacp.otyp.production, tfacp.otyp.service, tfacp.otyp.maint.sales, tfacp.otyp.pcs.project, tfacp.otyp.maint.work, tfacp.otyp.not.applicable, tfacp.otyp.proj.contract, tfacp.otyp.customer.claim, tfacp.otyp.intercomp.trade, tfacp.otyp.services.procm |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Obsolete. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Obsolete |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Obsolete |
| `load` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `shpi` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `clus` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `logd` | `date` | `date` |  |  | Obsolete |
| `logt` | `integer` | `int` |  |  | Obsolete |
| `user` | `string` | `varchar` |  |  | Obsolete. Max length: 16 |
| `byer` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `mess__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 132 |
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
