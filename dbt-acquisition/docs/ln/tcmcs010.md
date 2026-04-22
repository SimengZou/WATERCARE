# tcmcs010

LN tcmcs010 Countries table, Generated 2026-04-10T19:41:09Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ccty` |
| **Column count** | 55 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ccty` | `string` | `varchar` | `PK` | `not_null` | (required) Country. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `ictc` | `string` | `varchar` |  |  | ISO Code alpha-3. Max length: 3 |
| `tfcd` | `string` | `varchar` |  |  | Telephone. Max length: 6 |
| `txcd` | `string` | `varchar` |  |  | Telex. Max length: 6 |
| `fxcd` | `string` | `varchar` |  |  | Fax. Max length: 6 |
| `ssgn` | `string` | `varchar` |  |  | Starting Sign for City/Area Code. Max length: 1 |
| `esgn` | `string` | `varchar` |  |  | Ending Sign for City/Area Code. Max length: 1 |
| `xsgn` | `string` | `varchar` |  |  | Sign for Extension. Max length: 1 |
| `meec` | `integer` | `int` |  |  | EU Member State. Allowed values: 1, 2 |
| `meec_kw` | `string` | `varchar` |  |  | EU Member State (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `geoc` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `pltx` | `integer` | `int` |  |  | Print Line Tax. Allowed values: 1, 2 |
| `pltx_kw` | `string` | `varchar` |  |  | Print Line Tax (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ptta` | `integer` | `int` |  |  | Print Tax by Tax Authority. Allowed values: 1, 2 |
| `ptta_kw` | `string` | `varchar` |  |  | Print Tax by Tax Authority (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txmp` | `integer` | `int` |  |  | Print Tax Exemption. Allowed values: 1, 2 |
| `txmp_kw` | `string` | `varchar` |  |  | Print Tax Exemption (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `coaf` | `string` | `varchar` |  |  | Address Format. Max length: 3 |
| `afal` | `string` | `varchar` |  |  | Address Format for Address Lines. Max length: 3 |
| `zmsk` | `string` | `varchar` |  |  | ZIP/Postal Code Mask. Max length: 10 |
| `ppcd` | `integer` | `int` |  |  | ZIP Code Base |
| `cgp1` | `string` | `varchar` |  |  | BLWI Country Group. Max length: 3 |
| `cgp2` | `string` | `varchar` |  |  | Tax Country Group. Max length: 3 |
| `vnch` | `integer` | `int` |  |  | VAT Number Check. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 |
| `vnch_kw` | `string` | `varchar` |  |  | VAT Number Check (keyword). Allowed values: tcvnch.atu, tcvnch.be, tcvnch.dk, tcvnch.fi, tcvnch.fr, tcvnch.de, tcvnch.el, tcvnch.ie, tcvnch.it, tcvnch.lu, tcvnch.nl, tcvnch.pt, tcvnch.es, tcvnch.se, tcvnch.gb, tcvnch.none |
| `bnch` | `integer` | `int` |  |  | Check on Bank Account No.. Allowed values: 1, 2, 4, 5, 6, 7 |
| `bnch_kw` | `string` | `varchar` |  |  | Check on Bank Account No. (keyword). Allowed values: tccom.bnch.proof11, tccom.bnch.proof97, tccom.bnch.proof11.nor, tccom.bnch.proof10.nor, tccom.bnch.proofk11, tccom.bnch.none |
| `awtn` | `integer` | `int` |  |  | Allow Wrong Tax Number. Allowed values: 1, 2 |
| `awtn_kw` | `string` | `varchar` |  |  | Allow Wrong Tax Number (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tzid` | `string` | `varchar` |  |  | Time Zone. Max length: 3 |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `ict2` | `string` | `varchar` |  |  | ISO Code alpha-2. Max length: 2 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `natl__en_us` | `string` | `varchar` |  | `not_null` | (required) Nationality - base language. Max length: 40 |
| `ivrc` | `string` | `varchar` |  |  | International Vehicle Registration Code. Max length: 3 |
| `uarc` | `integer` | `int` |  |  | Use Arrival Confirmation. Allowed values: 1, 2 |
| `uarc_kw` | `string` | `varchar` |  |  | Use Arrival Confirmation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `coaf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom135 Address Formats |
| `afal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom135 Address Formats |
| `cgp1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs180 Country Groups |
| `cgp2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs180 Country Groups |
| `tzid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm100 Time Zones |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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
