# tfgld007

LN tfgld007 Period Status table, Generated 2026-04-10T19:41:41Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld007` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ptyp`, `year`, `prno` |
| **Column count** | 36 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ptyp` | `integer` | `int` | `PK` | `not_null` | (required) Period Type. Allowed values: 1, 2, 3 |
| `ptyp_kw` | `string` | `varchar` |  |  | Period Type (keyword). Allowed values: tfgld.ptyp.financial, tfgld.ptyp.reporting, tfgld.ptyp.vat |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `prno` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `sacp` | `integer` | `int` |  |  | Period Status ACP. Allowed values: 1, 2, 3 |
| `sacp_kw` | `string` | `varchar` |  |  | Period Status ACP (keyword). Allowed values: tfgld.sper.open, tfgld.sper.closed, tfgld.sper.finally.closed |
| `dacp` | `timestamp` | `timestamp_ntz` |  |  | Change Date ACP |
| `uacp` | `string` | `varchar` |  |  | User ACP. Max length: 16 |
| `sacr` | `integer` | `int` |  |  | Period Status ACR. Allowed values: 1, 2, 3 |
| `sacr_kw` | `string` | `varchar` |  |  | Period Status ACR (keyword). Allowed values: tfgld.sper.open, tfgld.sper.closed, tfgld.sper.finally.closed |
| `dacr` | `timestamp` | `timestamp_ntz` |  |  | Change Date ACR |
| `uacr` | `string` | `varchar` |  |  | User ACR. Max length: 16 |
| `scmg` | `integer` | `int` |  |  | Period Status CMG. Allowed values: 1, 2, 3 |
| `scmg_kw` | `string` | `varchar` |  |  | Period Status CMG (keyword). Allowed values: tfgld.sper.open, tfgld.sper.closed, tfgld.sper.finally.closed |
| `dcmg` | `timestamp` | `timestamp_ntz` |  |  | Change Date CMG |
| `ucmg` | `string` | `varchar` |  |  | User CMG. Max length: 16 |
| `sgld` | `integer` | `int` |  |  | Period Status GLD. Allowed values: 1, 2, 3 |
| `sgld_kw` | `string` | `varchar` |  |  | Period Status GLD (keyword). Allowed values: tfgld.sper.open, tfgld.sper.closed, tfgld.sper.finally.closed |
| `dgld` | `timestamp` | `timestamp_ntz` |  |  | Change Date GLD |
| `ugld` | `string` | `varchar` |  |  | User GLD. Max length: 16 |
| `sint` | `integer` | `int` |  |  | Period Status Integration. Allowed values: 1, 2, 3 |
| `sint_kw` | `string` | `varchar` |  |  | Period Status Integration (keyword). Allowed values: tfgld.sper.open, tfgld.sper.closed, tfgld.sper.finally.closed |
| `dint` | `timestamp` | `timestamp_ntz` |  |  | Change Date Integrations |
| `uint` | `string` | `varchar` |  |  | User Integrations. Max length: 16 |
| `ptyp_year_prno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld005 Periods |
| `year_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld006 End Dates by Year |
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
