# tcemm170

LN tcemm170 Companies table, Generated 2026-04-10T19:41:07Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcemm170` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `comp` |
| **Column count** | 60 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `comp` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `ctyp` | `integer` | `int` |  |  | Company Type. Allowed values: 1, 2, 3 |
| `ctyp_kw` | `string` | `varchar` |  |  | Company Type (keyword). Allowed values: tcemm.ctyp.logistic, tcemm.ctyp.financial, tcemm.ctyp.both |
| `csys` | `integer` | `int` |  |  | Currency System. Allowed values: 1, 2, 3, 4 |
| `csys_kw` | `string` | `varchar` |  |  | Currency System (keyword). Allowed values: tcemm.depe.single, tcemm.depe.dependent, tcemm.depe.independent, tcemm.depe.standard |
| `fcua` | `string` | `varchar` |  |  | Local Currency. Max length: 3 |
| `ctya` | `integer` | `int` |  |  | Currency Type. Allowed values: 0, 1, 2 |
| `ctya_kw` | `string` | `varchar` |  |  | Currency Type (keyword). Allowed values: , tcemm.curt.local, tcemm.curt.reporting |
| `lcur` | `string` | `varchar` |  |  | Logistic Currency. Max length: 3 |
| `fcub` | `string` | `varchar` |  |  | Reporting Currency 1. Max length: 3 |
| `ctyb` | `integer` | `int` |  |  | Currency Type. Allowed values: 0, 1, 2 |
| `ctyb_kw` | `string` | `varchar` |  |  | Currency Type (keyword). Allowed values: , tcemm.curt.local, tcemm.curt.reporting |
| `fcuc` | `string` | `varchar` |  |  | Reporting Currency 2. Max length: 3 |
| `ctyc` | `integer` | `int` |  |  | Currency Type. Allowed values: 0, 1, 2 |
| `ctyc_kw` | `string` | `varchar` |  |  | Currency Type (keyword). Allowed values: , tcemm.curt.local, tcemm.curt.reporting |
| `umfc` | `integer` | `int` |  |  | Use Multiple Functional Currencies. Allowed values: 1, 2 |
| `umfc_kw` | `string` | `varchar` |  |  | Use Multiple Functional Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clcu` | `string` | `varchar` |  |  | Credit Limit Currency. Max length: 3 |
| `expu` | `string` | `varchar` |  |  | Exchange Rate Type Purchase. Max length: 3 |
| `exsa` | `string` | `varchar` |  |  | Exchange Rate Type Sales. Max length: 3 |
| `exeu` | `string` | `varchar` |  |  | Exchange Rate Type Internal. Max length: 3 |
| `exex` | `string` | `varchar` |  |  | Exchange Rate Type External. Max length: 3 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `tzid` | `string` | `varchar` |  |  | Time Zone Code. Max length: 3 |
| `euro` | `string` | `varchar` |  |  | Transition Currency. Max length: 3 |
| `tmub` | `integer` | `int` |  |  | Translation Method 1. Allowed values: 1, 2, 3 |
| `tmub_kw` | `string` | `varchar` |  |  | Translation Method 1 (keyword). Allowed values: tcemm.tmrc.transaction, tcemm.tmrc.local, tcemm.tmrc.not.applicable |
| `rdub` | `integer` | `int` |  |  | Rate Determination Method 1. Allowed values: 1, 2, 3, 10 |
| `rdub_kw` | `string` | `varchar` |  |  | Rate Determination Method 1 (keyword). Allowed values: tcrdrc.adopt, tcrdrc.own, tcrdrc.default, tcrdrc.not.applicable |
| `erub` | `string` | `varchar` |  |  | Exchange Rate Type 1. Max length: 3 |
| `tmuc` | `integer` | `int` |  |  | Translation Method 2. Allowed values: 1, 2, 3 |
| `tmuc_kw` | `string` | `varchar` |  |  | Translation Method 2 (keyword). Allowed values: tcemm.tmrc.transaction, tcemm.tmrc.local, tcemm.tmrc.not.applicable |
| `rduc` | `integer` | `int` |  |  | Rate Determination Method 2. Allowed values: 1, 2, 3, 10 |
| `rduc_kw` | `string` | `varchar` |  |  | Rate Determination Method 2 (keyword). Allowed values: tcrdrc.adopt, tcrdrc.own, tcrdrc.default, tcrdrc.not.applicable |
| `eruc` | `string` | `varchar` |  |  | Exchange Rate Type 2. Max length: 3 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `fcua_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `lcur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `fcub_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `fcuc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `clcu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `tzid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm100 Time Zones |
| `euro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `erub_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `eruc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `dcur_comp` | `string` | `varchar` |  |  | CC: Data Warehouse Currency of Company. Max length: 3 |
| `taxo_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Taxonomy |
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
