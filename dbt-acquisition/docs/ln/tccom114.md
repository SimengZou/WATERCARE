# tccom114

LN tccom114 Pay-by Business Partners table, Generated 2026-04-10T19:41:02Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom114` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pfbp`, `cofc` |
| **Column count** | 73 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pfbp` | `string` | `varchar` | `PK` | `not_null` | (required) Pay-by Business Partner. Max length: 9 |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `bpst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 254 |
| `bpst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.bpst.active, tccom.bpst.inactive, tccom.bpst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
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
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cban` | `string` | `varchar` |  |  | Bank. Max length: 3 |
| `bank` | `string` | `varchar` |  |  | Bank Relation. Max length: 3 |
| `eded` | `integer` | `int` |  |  | Extra Days after Due Date |
| `mrem` | `string` | `varchar` |  |  | Reminder Method. Max length: 3 |
| `rtad` | `string` | `varchar` |  |  | Remit-to Address. Max length: 9 |
| `prra` | `integer` | `int` |  |  | Print Receipt Acknowledgement. Allowed values: 1, 2 |
| `prra_kw` | `string` | `varchar` |  |  | Print Receipt Acknowledgement (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vtcb` | `integer` | `int` |  |  | Vat on Cash Basis. Allowed values: 1, 2 |
| `vtcb_kw` | `string` | `varchar` |  |  | Vat on Cash Basis (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stxa` | `integer` | `int` |  |  | Revenue Stamp Tax Applicable. Allowed values: 1, 2 |
| `stxa_kw` | `string` | `varchar` |  |  | Revenue Stamp Tax Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tndm` | `integer` | `int` |  |  | Trade Note Division Method. Allowed values: 1, 2, 3, 254 |
| `tndm_kw` | `string` | `varchar` |  |  | Trade Note Division Method (keyword). Allowed values: tctndm.stamptax, tctndm.predetermined, tctndm.na, tctndm.not.specified |
| `mxtn` | `integer` | `int` |  |  | Maximum Number of TNs Allowed |
| `ppsl` | `integer` | `int` |  |  | Print Payment Slip. Allowed values: 1, 2 |
| `ppsl_kw` | `string` | `varchar` |  |  | Print Payment Slip (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aofc` | `string` | `varchar` |  |  | Accounting Office. Max length: 6 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `setl` | `integer` | `int` |  |  | Block Settlement of Direct Debits with Purchase Invoices. Allowed values: 1, 2 |
| `setl_kw` | `string` | `varchar` |  |  | Block Settlement of Direct Debits with Purchase Invoices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `pfbp_cban_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom115 Bank Accounts by Pay-by Business Partner |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `rtad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
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
