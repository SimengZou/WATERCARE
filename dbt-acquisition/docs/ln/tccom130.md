# tccom130

LN tccom130 Addresses table, Generated 2026-04-10T19:41:04Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom130` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cadr` |
| **Column count** | 79 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cadr` | `string` | `varchar` | `PK` | `not_null` | (required) Address Code. Max length: 9 |
| `nama__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 35 |
| `namb__en_us` | `string` | `varchar` |  | `not_null` | (required) Name 2 - base language. Max length: 30 |
| `namc__en_us` | `string` | `varchar` |  | `not_null` | (required) Street - base language. Max length: 30 |
| `namd__en_us` | `string` | `varchar` |  | `not_null` | (required) Street 2 - base language. Max length: 30 |
| `hono__en_us` | `string` | `varchar` |  | `not_null` | (required) House Number - base language. Max length: 10 |
| `pobn__en_us` | `string` | `varchar` |  | `not_null` | (required) P.O. Box Number - base language. Max length: 10 |
| `namf__en_us` | `string` | `varchar` |  | `not_null` | (required) City 2 - base language. Max length: 30 |
| `pstc__en_us` | `string` | `varchar` |  | `not_null` | (required) ZIP Code/Postal Code - base language. Max length: 10 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `telp` | `string` | `varchar` |  |  | Telephone. Max length: 32 |
| `telx` | `string` | `varchar` |  |  | Telex. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax. Max length: 32 |
| `ccit` | `string` | `varchar` |  |  | City. Max length: 8 |
| `ccty` | `string` | `varchar` |  |  | Country. Max length: 3 |
| `cste` | `string` | `varchar` |  |  | State/Province. Max length: 3 |
| `bldg__en_us` | `string` | `varchar` |  | `not_null` | (required) Building - base language. Max length: 100 |
| `blfl__en_us` | `string` | `varchar` |  | `not_null` | (required) Floor - base language. Max length: 30 |
| `blun__en_us` | `string` | `varchar` |  | `not_null` | (required) Unit - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `fovn` | `string` | `varchar` |  |  | Obsolete. Max length: 24 |
| `lvdt` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `coaf` | `string` | `varchar` |  |  | Address Format. Max length: 3 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `gebo` | `integer` | `int` |  |  | GEO Code based on. Allowed values: 10, 20, 100 |
| `gebo_kw` | `string` | `varchar` |  |  | GEO Code based on (keyword). Allowed values: tctax.gebo.address, tctax.gebo.coordinates, tctax.gebo.not.appl |
| `geoc` | `string` | `varchar` |  |  | GEO Code. Max length: 10 |
| `cnty__en_us` | `string` | `varchar` |  | `not_null` | (required) County - base language. Max length: 30 |
| `dtlm` | `timestamp` | `timestamp_ntz` |  |  | Date Last Modification |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `tzid` | `string` | `varchar` |  |  | Time Zone. Max length: 3 |
| `inet__en_us` | `string` | `varchar` |  | `not_null` | (required) Website - base language. Max length: 150 |
| `etyp` | `integer` | `int` |  |  | E-Mail Address Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 10 |
| `etyp_kw` | `string` | `varchar` |  |  | E-Mail Address Type (keyword). Allowed values: tccom.etyp.smtp, tccom.etyp.x400, tccom.etyp.notes, tccom.etyp.ccmail, tccom.etyp.mhs, tccom.etyp.mess, tccom.etyp.ms, tccom.etyp.not.applicable |
| `foid` | `integer` | `int` |  |  | Front Office ID |
| `crid` | `integer` | `int` |  |  | Creator's Unique System ID |
| `sita` | `string` | `varchar` |  |  | SITA Address. Max length: 7 |
| `info__en_us` | `string` | `varchar` |  | `not_null` | (required) E-mail - base language. Max length: 100 |
| `smsa__en_us` | `string` | `varchar` |  | `not_null` | (required) SMS Number - base language. Max length: 50 |
| `defa` | `integer` | `int` |  |  | Default Messaging. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `defa_kw` | `string` | `varchar` |  |  | Default Messaging (keyword). Allowed values: tccom.defa.email, tccom.defa.fax, tccom.defa.telex, tccom.defa.sita, tccom.defa.sms, tccom.defa.phone, tccom.defa.not.applicable |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) City Description - base language. Max length: 30 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `vatl` | `string` | `varchar` |  |  | Local Tax Number. Max length: 25 |
| `glat` | `float` | `float` |  |  | GPS Latitude (WGS84) |
| `glon` | `float` | `float` |  |  | GPS Longitude (WGS84) |
| `icty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `valr` | `integer` | `int` |  |  | Validation Required. Allowed values: 1, 2 |
| `valr_kw` | `string` | `varchar` |  |  | Validation Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `afal` | `string` | `varchar` |  |  | Address Format for Address Lines. Max length: 3 |
| `ln01__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 1 - base language. Max length: 100 |
| `ln02__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 2 - base language. Max length: 100 |
| `ln03__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 3 - base language. Max length: 100 |
| `ln04__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 4 - base language. Max length: 100 |
| `ln05__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 5 - base language. Max length: 100 |
| `ln06__en_us` | `string` | `varchar` |  | `not_null` | (required) Address Line 6 - base language. Max length: 100 |
| `ezty` | `string` | `varchar` |  |  | Economic Zone Type. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs143 States/Provinces |
| `ccty_cste_ccit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom139 Cities by Country |
| `coaf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom135 Address Formats |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `tzid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm100 Time Zones |
| `afal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom135 Address Formats |
| `ezty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom187 Economic Zone Types |
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
