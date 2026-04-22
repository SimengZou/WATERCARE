# tsbsc100

LN tsbsc100 Installation Group table, Generated 2026-04-10T19:42:28Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsbsc100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `clst` |
| **Column count** | 96 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `clst` | `string` | `varchar` | `PK` | `not_null` | (required) Installation Group. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `sear__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Argument - base language. Max length: 16 |
| `exin__en_us` | `string` | `varchar` |  | `not_null` | (required) Extra Information - base language. Max length: 30 |
| `ofbp` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Owner Contact. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Owner Address. Max length: 9 |
| `ubbp` | `string` | `varchar` |  |  | In Use-by Business Partner. Max length: 9 |
| `ubad` | `string` | `varchar` |  |  | In Use-by Business Partner Address. Max length: 9 |
| `ubcn` | `string` | `varchar` |  |  | In Use-by Business Partner Contact. Max length: 9 |
| `ubop` | `string` | `varchar` |  |  | In Use-by Business Partner Operator Contact. Max length: 9 |
| `ubpc` | `integer` | `int` |  |  | In Use-by Primary Contact. Allowed values: 10, 20 |
| `ubpc_kw` | `string` | `varchar` |  |  | In Use-by Primary Contact (keyword). Allowed values: tscfg.pcnt.in.use.by, tscfg.pcnt.operator |
| `dler` | `string` | `varchar` |  |  | Dealer. Max length: 9 |
| `dlad` | `string` | `varchar` |  |  | Dealer Address. Max length: 9 |
| `dlcn` | `string` | `varchar` |  |  | Dealer Contact. Max length: 9 |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `bfad` | `string` | `varchar` |  |  | Buy-from Address. Max length: 9 |
| `bfcn` | `string` | `varchar` |  |  | Buy-from Contact. Max length: 9 |
| `owtp` | `integer` | `int` |  |  | Type of Ownership. Allowed values: 1, 2 |
| `owtp_kw` | `string` | `varchar` |  |  | Type of Ownership (keyword). Allowed values: tscfg.owtp.external, tscfg.owtp.internal |
| `dbpo` | `integer` | `int` |  |  | Default Business Partner for Order. Allowed values: 1, 2, 3, 4 |
| `dbpo_kw` | `string` | `varchar` |  |  | Default Business Partner for Order (keyword). Allowed values: tscfg.dbpo.owner, tscfg.dbpo.in.use.by, tscfg.dbpo.dealer, tscfg.dbpo.supplier |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `odpt` | `string` | `varchar` |  |  | Department. Max length: 6 |
| `soff` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `mdpt` | `string` | `varchar` |  |  | Operations Department. Max length: 6 |
| `rste` | `string` | `varchar` |  |  | Repair Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `lste` | `string` | `varchar` |  |  | Location Site. Max length: 9 |
| `lcad` | `string` | `varchar` |  |  | Location Address. Max length: 9 |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `trdt` | `float` | `float` |  |  | Travel Distance |
| `trtm` | `float` | `float` |  |  | Travel Time |
| `czon` | `string` | `varchar` |  |  | Distance Zone. Max length: 3 |
| `ccha` | `float` | `float` |  |  | Call-out Charge |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cusc` | `string` | `varchar` |  |  | Usage Class. Max length: 3 |
| `mdnr` | `string` | `varchar` |  |  | Modem. Max length: 32 |
| `prfa` | `string` | `varchar` |  |  | Preferred Engineer Field Service. Max length: 9 |
| `prfb` | `string` | `varchar` |  |  | Preferred Engineer Depot Repair. Max length: 9 |
| `prio` | `integer` | `int` |  |  | Priority |
| `ccal` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `rspe` | `string` | `varchar` |  |  | Responsible Engineer. Max length: 9 |
| `eftm` | `timestamp` | `timestamp_ntz` |  |  | Effective Time |
| `clrt` | `string` | `varchar` |  |  | Labor Rate Code. Max length: 6 |
| `ortp` | `integer` | `int` |  |  | Originating Order Type. Allowed values: 5, 10, 15, 99 |
| `ortp_kw` | `string` | `varchar` |  |  | Originating Order Type (keyword). Allowed values: tscfg.ortp.sales, tscfg.ortp.deliverable, tscfg.ortp.project, tscfg.ortp.not.applicable |
| `srno` | `string` | `varchar` |  |  | Originating Order. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Originating Contract Line. Max length: 8 |
| `spno` | `integer` | `int` |  |  | Originating Order Line |
| `sdno` | `integer` | `int` |  |  | Originating Detail Line |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ubbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ubad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ubcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ubop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `dler_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `dlad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `dlcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `bfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `bfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `bfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `odpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `soff_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `mdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `rste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `lste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `lcad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `czon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm120 Distance Zones |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cusc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsspc030 Usage Classes |
| `prfa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `prfb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `prio_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `rspe_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
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
