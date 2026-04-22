# bpmdm001

LN bpmdm001 Employee People Data table, Generated 2026-04-10T19:40:58Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_bpmdm001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `emno` |
| **Column count** | 53 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `emno` | `string` | `varchar` | `PK` | `not_null` | (required) Employee. Max length: 9 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `hwem` | `float` | `float` |  |  | Employment |
| `wtsc` | `string` | `varchar` |  |  | Working Time Schedule. Max length: 3 |
| `sdte` | `date` | `date` |  |  | First Date of Employment |
| `edte` | `date` | `date` |  |  | Last Date of Employment |
| `daob` | `date` | `date` |  |  | Date of Birth |
| `sexe` | `integer` | `int` |  |  | Gender. Allowed values: 1, 2, 3, 100 |
| `sexe_kw` | `string` | `varchar` |  |  | Gender (keyword). Allowed values: tcsexe.male, tcsexe.female, tcsexe.not.known, tcsexe.not.appl |
| `cist` | `string` | `varchar` |  |  | Civil Status. Max length: 15 |
| `emtp` | `integer` | `int` |  |  | Employee Type. Allowed values: 1, 2 |
| `emtp_kw` | `string` | `varchar` |  |  | Employee Type (keyword). Allowed values: bpmdm.emtp.internal, bpmdm.emtp.external |
| `finr` | `string` | `varchar` |  |  | Fiscal Number. Max length: 16 |
| `telw` | `string` | `varchar` |  |  | Telephone. Max length: 32 |
| `tlw1` | `string` | `varchar` |  |  | Telephone2. Max length: 32 |
| `tefw` | `string` | `varchar` |  |  | Fax. Max length: 32 |
| `mail__en_us` | `string` | `varchar` |  | `not_null` | (required) E-Mail - base language. Max length: 100 |
| `ucrm` | `integer` | `int` |  |  | Create CRM Appointments for Assignments. Allowed values: 1, 2 |
| `ucrm_kw` | `string` | `varchar` |  |  | Create CRM Appointments for Assignments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `telm` | `string` | `varchar` |  |  | Mobile Phone. Max length: 32 |
| `msty` | `integer` | `int` |  |  | Messenger Type. Allowed values: 10, 20, 30, 40, 50, 60 |
| `msty_kw` | `string` | `varchar` |  |  | Messenger Type (keyword). Allowed values: tccom.mestype.na, tccom.mestype.skype, tccom.mestype.msn, tccom.mestype.aol, tccom.mestype.yahoo, tccom.mestype.lync |
| `msad__en_us` | `string` | `varchar` |  | `not_null` | (required) Messenger Address - base language. Max length: 50 |
| `jobt__en_us` | `string` | `varchar` |  | `not_null` | (required) Job Title - base language. Max length: 50 |
| `bano` | `string` | `varchar` |  |  | Bank Account. Max length: 34 |
| `tctr__en_us` | `string` | `varchar` |  | `not_null` | (required) Type of Employment Contract - base language. Max length: 30 |
| `cedt` | `date` | `date` |  |  | Contract End Date |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `ccty` | `string` | `varchar` |  |  | Country of Nationality. Max length: 3 |
| `rgno__en_us` | `string` | `varchar` |  | `not_null` | (required) Registration Number - base language. Max length: 30 |
| `prtn__en_us` | `string` | `varchar` |  | `not_null` | (required) Partner - base language. Max length: 35 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `supv` | `string` | `varchar` |  |  | Supervisor. Max length: 9 |
| `ctrg` | `string` | `varchar` |  |  | Trade Group. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Employee Text |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `wtsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl101 Working Time Schedule Codes |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `supv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
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
