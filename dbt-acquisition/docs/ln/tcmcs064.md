# tcmcs064

LN tcmcs064 Credit Ratings table, Generated 2026-04-10T19:41:13Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs064` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `crat` |
| **Column count** | 41 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `crat` | `string` | `varchar` | `PK` | `not_null` | (required) Credit Rating. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `phs1` | `integer` | `int` |  |  | Credit Check Sales Order Entry. Allowed values: 1, 2 |
| `phs1_kw` | `string` | `varchar` |  |  | Credit Check Sales Order Entry (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs2` | `integer` | `int` |  |  | Credit Check Release to Warehousing. Allowed values: 1, 2 |
| `phs2_kw` | `string` | `varchar` |  |  | Credit Check Release to Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs3` | `integer` | `int` |  |  | Credit Check Confirm Shipment. Allowed values: 1, 2 |
| `phs3_kw` | `string` | `varchar` |  |  | Credit Check Confirm Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs4` | `integer` | `int` |  |  | Contract Line Entry. Allowed values: 1, 2 |
| `phs4_kw` | `string` | `varchar` |  |  | Contract Line Entry (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs5` | `integer` | `int` |  |  | Contract Deliverable Entry. Allowed values: 1, 2 |
| `phs5_kw` | `string` | `varchar` |  |  | Contract Deliverable Entry (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs6` | `integer` | `int` |  |  | Release to Warehousing. Allowed values: 1, 2 |
| `phs6_kw` | `string` | `varchar` |  |  | Release to Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phs7` | `integer` | `int` |  |  | Confirm Shipment. Allowed values: 1, 2 |
| `phs7_kw` | `string` | `varchar` |  |  | Confirm Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pss1` | `integer` | `int` |  |  | Sales Schedule Approval. Allowed values: 1, 2 |
| `pss1_kw` | `string` | `varchar` |  |  | Sales Schedule Approval (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pss2` | `integer` | `int` |  |  | Sales Schedule Release to Warehousing. Allowed values: 1, 2 |
| `pss2_kw` | `string` | `varchar` |  |  | Sales Schedule Release to Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pss3` | `integer` | `int` |  |  | Sales Schedule Confirm Shipment. Allowed values: 1, 2 |
| `pss3_kw` | `string` | `varchar` |  |  | Sales Schedule Confirm Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bldf` | `string` | `varchar` |  |  | Sales Schedule Blocking Definition. Max length: 3 |
| `actn` | `integer` | `int` |  |  | Action. Allowed values: 1, 2, 3, 4 |
| `actn_kw` | `string` | `varchar` |  |  | Action (keyword). Allowed values: tcmcs.actn.always.credit, tcmcs.actn.never.credit, tcmcs.actn.check.credit, tcmcs.actn.always.over.inv |
| `revp` | `integer` | `int` |  |  | Review Credit Rating after |
| `ioso` | `integer` | `int` |  |  | Include Open Order Balance in Financial Risk Calculation. Allowed values: 1, 2 |
| `ioso_kw` | `string` | `varchar` |  |  | Include Open Order Balance in Financial Risk Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ddcc` | `integer` | `int` |  |  | Central Direct Delivery Credit Checking. Allowed values: 1, 2 |
| `ddcc_kw` | `string` | `varchar` |  |  | Central Direct Delivery Credit Checking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bldf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs211 Blocking Definitions |
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
