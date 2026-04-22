# tcmcs041

LN tcmcs041 Delivery Terms table, Generated 2026-04-10T19:41:11Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs041` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cdec` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cdec` | `string` | `varchar` | `PK` | `not_null` | (required) Delivery Terms. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cdyn` | `integer` | `int` |  |  | Cash on Delivery. Allowed values: 1, 2 |
| `cdyn_kw` | `string` | `varchar` |  |  | Cash on Delivery (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crpd` | `integer` | `int` |  |  | Carriage Paid. Allowed values: 1, 2 |
| `crpd_kw` | `string` | `varchar` |  |  | Carriage Paid (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cptp` | `integer` | `int` |  |  | Carriage Paid by 3rd Party. Allowed values: 1, 2 |
| `cptp_kw` | `string` | `varchar` |  |  | Carriage Paid by 3rd Party (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fcba` | `string` | `varchar` |  |  | Freight Charges Bill-To Address. Max length: 9 |
| `potp` | `integer` | `int` |  |  | Point of Title Passage. Allowed values: 1, 2, 10 |
| `potp_kw` | `string` | `varchar` |  |  | Point of Title Passage (keyword). Allowed values: tcpotp.point.of.orig, tcpotp.point.of.dest, tcpotp.named.loc |
| `tdgp` | `integer` | `int` |  |  | Delivery Terms Group. Allowed values: 5, 10, 15, 20, 25, 27, 28, 29, 30, 35, 40, 45, 50, 55, 60, 65, 70, 90 |
| `tdgp_kw` | `string` | `varchar` |  |  | Delivery Terms Group (keyword). Allowed values: tctdgp.cif, tctdgp.cfr, tctdgp.cpt, tctdgp.cip, tctdgp.daf, tctdgp.dap, tctdgp.dpu, tctdgp.dat, tctdgp.deq, tctdgp.des, tctdgp.ddu, tctdgp.ddp, tctdgp.exw, tctdgp.fca, tctdgp.fas, tctdgp.fob, tctdgp.oth, tctdgp.na |
| `spec` | `integer` | `int` |  |  | Point of Title Passage Specification. Allowed values: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 125, 127 |
| `spec_kw` | `string` | `varchar` |  |  | Point of Title Passage Specification (keyword). Allowed values: tcmcs.spec.adnm, tcmcs.spec.name2, tcmcs.spec.addr, tcmcs.spec.addr2, tcmcs.spec.city, tcmcs.spec.city2, tcmcs.spec.prov, tcmcs.spec.ccty, tcmcs.spec.adnm.prov.ccty, tcmcs.spec.addr2.prov.ccty, tcmcs.spec.city.prov.ccty, tcmcs.spec.city2.prov.ccty, tcmcs.spec.named.loc, tcmcs.spec.not.appl |
| `txta` | `integer` | `int` |  |  | Text |
| `fcba_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
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
