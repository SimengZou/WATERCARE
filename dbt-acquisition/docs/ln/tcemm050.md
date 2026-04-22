# tcemm050

LN tcemm050 Sites table, Generated 2026-04-10T19:41:06Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcemm050` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `site` |
| **Column count** | 57 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `site` | `string` | `varchar` | `PK` | `not_null` | (required) Site. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `stat` | `integer` | `int` |  |  | Obsolete. Allowed values: 10, 20, 30, 40, 50, 100 |
| `stat_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcemm.stat.in.prep, tcemm.stat.active, tcemm.stat.closing.in.prog, tcemm.stat.closed, tcemm.stat.hidden, tcemm.stat.not.appl |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `cadr` | `string` | `varchar` |  |  | Address. Max length: 9 |
| `admo` | `integer` | `int` |  |  | Administrative Only. Allowed values: 1, 2 |
| `admo_kw` | `string` | `varchar` |  |  | Administrative Only (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clus` | `string` | `varchar` |  |  | Planning Cluster. Max length: 3 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `eunt` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Default Warehouse. Max length: 6 |
| `xsit` | `integer` | `int` |  |  | External Site. Allowed values: 1, 2 |
| `xsit_kw` | `string` | `varchar` |  |  | External Site (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `subs` | `integer` | `int` |  |  | Subcontractor Site. Allowed values: 1, 2 |
| `subs_kw` | `string` | `varchar` |  |  | Subcontractor Site (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `cust` | `integer` | `int` |  |  | Customer Site. Allowed values: 1, 2 |
| `cust_kw` | `string` | `varchar` |  |  | Customer Site (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `expl` | `integer` | `int` |  |  | Exclude From Enterprise Planning. Allowed values: 1, 2 |
| `expl_kw` | `string` | `varchar` |  |  | Exclude From Enterprise Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scat` | `string` | `varchar` |  |  | Site Category. Max length: 9 |
| `psit` | `string` | `varchar` |  |  | Parent Site. Max length: 9 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `crby` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `inf1__en_us` | `string` | `varchar` |  | `not_null` | (required) Site Information 1 - base language. Max length: 100 |
| `inf2__en_us` | `string` | `varchar` |  | `not_null` | (required) Site Information 2 - base language. Max length: 100 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `clus_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm135 Clusters |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `eunt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm030 Enterprise Units |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `scat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm052 Site Categories |
| `psit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm055 Parent Sites |
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
