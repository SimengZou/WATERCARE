# tccom111

LN tccom111 Ship-to Business Partners table, Generated 2026-04-10T19:41:01Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom111` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `stbp` |
| **Column count** | 76 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `stbp` | `string` | `varchar` | `PK` | `not_null` | (required) Ship-to Business Partner. Max length: 9 |
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
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `apsr` | `integer` | `int` |  |  | Automatically Process Sales Schedule Releases. Allowed values: 1, 2 |
| `apsr_kw` | `string` | `varchar` |  |  | Automatically Process Sales Schedule Releases (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `capp` | `integer` | `int` |  |  | Customer Approval. Allowed values: 1, 2 |
| `capp_kw` | `string` | `varchar` |  |  | Customer Approval (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rrqt` | `integer` | `int` |  |  | Return Rejected Quantity. Allowed values: 1, 2 |
| `rrqt_kw` | `string` | `varchar` |  |  | Return Rejected Quantity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aetc` | `integer` | `int` |  |  | Authorize Excess Transportation Costs. Allowed values: 1, 2 |
| `aetc_kw` | `string` | `varchar` |  |  | Authorize Excess Transportation Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `mask` | `string` | `varchar` |  |  | Shipment Handling Unit Mask. Max length: 9 |
| `sscc` | `integer` | `int` |  |  | SSCC Standard. Allowed values: 1, 2 |
| `sscc_kw` | `string` | `varchar` |  |  | SSCC Standard (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `rdec` | `string` | `varchar` |  |  | Return Delivery Terms. Max length: 3 |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `rfia` | `integer` | `int` |  |  | Radio Frequency Identification Applicable. Allowed values: 1, 2 |
| `rfia_kw` | `string` | `varchar` |  |  | Radio Frequency Identification Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rfis` | `string` | `varchar` |  |  | Radio Frequency Identification Structure. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `mask_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `rdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
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
