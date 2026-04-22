# tcmcs003

LN tcmcs003 Warehouses table, Generated 2026-04-10T19:41:09Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs003` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar` |
| **Column count** | 54 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `typw` | `integer` | `int` |  |  | Warehouse Type. Allowed values: 1, 2, 5, 11, 13, 14, 21, 25, 50 |
| `typw_kw` | `string` | `varchar` |  |  | Warehouse Type (keyword). Allowed values: tctypw.normal, tctypw.wip, tctypw.project, tctypw.service, tctypw.srv.cust.owned, tctypw.srv.reject, tctypw.consignment, tctypw.consignment.own, tctypw.financial |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `bpid` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `comp` | `integer` | `int` |  |  | Financial Company |
| `wmsc` | `integer` | `int` |  |  | WMS Controlled. Allowed values: 1, 2 |
| `wmsc_kw` | `string` | `varchar` |  |  | WMS Controlled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mesc` | `integer` | `int` |  |  | MES Controlled. Allowed values: 1, 2 |
| `mesc_kw` | `string` | `varchar` |  |  | MES Controlled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `inep` | `integer` | `int` |  |  | Include in Enterprise Planning. Allowed values: 1, 2 |
| `inep_kw` | `string` | `varchar` |  |  | Include in Enterprise Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imgt` | `integer` | `int` |  |  | Inventory Management. Allowed values: 1, 2 |
| `imgt_kw` | `string` | `varchar` |  |  | Inventory Management (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imbp` | `string` | `varchar` |  |  | Inventory Management Business Partner. Max length: 9 |
| `xsit` | `integer` | `int` |  |  | External Site. Allowed values: 1, 2, 3 |
| `xsit_kw` | `string` | `varchar` |  |  | External Site (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `xsbp` | `string` | `varchar` |  |  | External Site Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `pwip` | `integer` | `int` |  |  | Project WIP Warehouse. Allowed values: 1, 2 |
| `pwip_kw` | `string` | `varchar` |  |  | Project WIP Warehouse (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `swhu` | `integer` | `int` |  |  | Shop Floor Warehouse Applicable for. Allowed values: 10, 20, 30, 100 |
| `swhu_kw` | `string` | `varchar` |  |  | Shop Floor Warehouse Applicable for (keyword). Allowed values: tcswhu.work.center, tcswhu.work.cell, tcswhu.line.station, tcswhu.not.appl |
| `duns` | `integer` | `int` |  |  | DUNS Number |
| `cdf_updt` | `timestamp` | `timestamp_ntz` |  |  | Last updated date |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `imbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `xsbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cwar_eunt` | `string` | `varchar` |  |  | CC: Enterprise Unit of Warehouse. Max length: 6 |
| `eunt_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Enterprise Unit |
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
