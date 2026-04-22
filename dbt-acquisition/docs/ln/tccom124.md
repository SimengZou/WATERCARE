# tccom124

LN tccom124 Pay-to Business Partners table, Generated 2026-04-10T19:41:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom124` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ptbp`, `cofc` |
| **Column count** | 81 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ptbp` | `string` | `varchar` | `PK` | `not_null` | (required) Pay-to Business Partner. Max length: 9 |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `bpst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 254 |
| `bpst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.bpst.active, tccom.bpst.inactive, tccom.bpst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `vryn` | `integer` | `int` |  |  | Vendor Rating. Allowed values: 1, 2 |
| `vryn_kw` | `string` | `varchar` |  |  | Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone Number. Max length: 32 |
| `tlcy` | `string` | `varchar` |  |  | Country for Phone Number. Max length: 6 |
| `tlci` | `string` | `varchar` |  |  | City/Area Code for Phone Number. Max length: 15 |
| `tlln` | `string` | `varchar` |  |  | Local Number for Phone Number. Max length: 32 |
| `tlex` | `string` | `varchar` |  |  | Extension for Phone Number. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax Number. Max length: 32 |
| `tfcy` | `string` | `varchar` |  |  | Country for Fax Number. Max length: 6 |
| `tfci` | `string` | `varchar` |  |  | City/Area Code for Fax Number. Max length: 15 |
| `tfln` | `string` | `varchar` |  |  | Local Number for Fax Number. Max length: 32 |
| `tfex` | `string` | `varchar` |  |  | Extension for Fax Number. Max length: 15 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `uptc` | `integer` | `int` |  |  | Use Pay-to Business Partner Currency. Allowed values: 1, 2 |
| `uptc_kw` | `string` | `varchar` |  |  | Use Pay-to Business Partner Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cban` | `string` | `varchar` |  |  | Bank. Max length: 3 |
| `bank` | `string` | `varchar` |  |  | Bank Relation. Max length: 3 |
| `eded` | `integer` | `int` |  |  | Extra Days after Due Date |
| `bcbs` | `integer` | `int` |  |  | Bank Charge Borne by Supplier. Allowed values: 1, 2 |
| `bcbs_kw` | `string` | `varchar` |  |  | Bank Charge Borne by Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stxa` | `integer` | `int` |  |  | Revenue Stamp Tax Applicable. Allowed values: 1, 2 |
| `stxa_kw` | `string` | `varchar` |  |  | Revenue Stamp Tax Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tndm` | `integer` | `int` |  |  | Trade Note Division Method. Allowed values: 1, 2, 3, 254 |
| `tndm_kw` | `string` | `varchar` |  |  | Trade Note Division Method (keyword). Allowed values: tctndm.stamptax, tctndm.predetermined, tctndm.na, tctndm.not.specified |
| `mxtn` | `integer` | `int` |  |  | Maximum Number of TNs Allowed |
| `aofc` | `string` | `varchar` |  |  | Accounting Office. Max length: 6 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cnsm` | `integer` | `int` |  |  | Constant Symbol |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `uwht` | `integer` | `int` |  |  | * obsolete *. Allowed values: 1, 2 |
| `uwht_kw` | `string` | `varchar` |  |  | * obsolete * (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `exor` | `integer` | `int` |  |  | * obsolete *. Allowed values: 1, 2 |
| `exor_kw` | `string` | `varchar` |  |  | * obsolete * (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `valf` | `timestamp` | `timestamp_ntz` |  |  | * obsolete * |
| `valt` | `timestamp` | `timestamp_ntz` |  |  | * obsolete * |
| `expo` | `integer` | `int` |  |  | * obsolete *. Allowed values: 1, 2 |
| `expo_kw` | `string` | `varchar` |  |  | * obsolete * (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txno__en_us` | `string` | `varchar` |  | `not_null` | (required) * obsolete * - base language. Max length: 20 |
| `vacc` | `string` | `varchar` |  |  | VAT Bank Account. Max length: 3 |
| `setl` | `integer` | `int` |  |  | Block Settlement of Payments with Sales Invoices. Allowed values: 1, 2 |
| `setl_kw` | `string` | `varchar` |  |  | Block Settlement of Payments with Sales Invoices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `ptbp_cban_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom125 Bank Accounts by Pay-to Business Partner |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `aofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
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
