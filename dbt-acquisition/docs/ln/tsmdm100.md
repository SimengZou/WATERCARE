# tsmdm100

LN tsmdm100 Service Offices table, Generated 2026-04-10T19:42:36Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsmdm100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwoc` |
| **Column count** | 125 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwoc` | `string` | `varchar` | `PK` | `not_null` | (required) Service Office. Max length: 6 |
| `wctp` | `integer` | `int` |  |  | Department Type. Allowed values: 5, 10 |
| `wctp_kw` | `string` | `varchar` |  |  | Department Type (keyword). Allowed values: tsmdm.wctp.main, tsmdm.wctp.sub |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cmwc` | `string` | `varchar` |  |  | Main Department. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `srtp` | `integer` | `int` |  |  | Field of Support. Allowed values: 5, 10 |
| `srtp_kw` | `string` | `varchar` |  |  | Field of Support (keyword). Allowed values: tsmdm.srtp.internal, tsmdm.srtp.external |
| `grcq` | `string` | `varchar` |  |  | Number Group Contract Quotes. Max length: 3 |
| `srep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `sncq` | `string` | `varchar` |  |  | Contract Quote Series Number. Max length: 8 |
| `grsc` | `string` | `varchar` |  |  | Number Group Contracts. Max length: 3 |
| `snsc` | `string` | `varchar` |  |  | Contract Series Number. Max length: 8 |
| `grsq` | `string` | `varchar` |  |  | Number Group Service Order Quotes. Max length: 3 |
| `snsq` | `string` | `varchar` |  |  | Service Order Quote Series Number. Max length: 8 |
| `grso` | `string` | `varchar` |  |  | Number Group Service Orders. Max length: 3 |
| `snso` | `string` | `varchar` |  |  | Service Order Series Number. Max length: 8 |
| `grca` | `string` | `varchar` |  |  | Number Group Calls. Max length: 3 |
| `snca` | `string` | `varchar` |  |  | Call Series Number. Max length: 8 |
| `grfc` | `string` | `varchar` |  |  | Number Group Field Change Order. Max length: 3 |
| `snfc` | `string` | `varchar` |  |  | Field Change Order Series Number. Max length: 8 |
| `grcl` | `string` | `varchar` |  |  | Number Group Customer Claim. Max length: 3 |
| `sncl` | `string` | `varchar` |  |  | Series Customer Claim. Max length: 8 |
| `ngsc` | `string` | `varchar` |  |  | Number Group Supplier Claim. Max length: 3 |
| `srsc` | `string` | `varchar` |  |  | Series Supplier Claim. Max length: 8 |
| `grwo` | `string` | `varchar` |  |  | Number Group Work Order. Max length: 3 |
| `snwo` | `string` | `varchar` |  |  | Work Order Series Number. Max length: 8 |
| `grms` | `string` | `varchar` |  |  | Number Group Maintenance Sales Order. Max length: 3 |
| `snms` | `string` | `varchar` |  |  | Maintenance Sales Order Series Number. Max length: 8 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `lcip` | `string` | `varchar` |  |  | Location Incoming Parts. Max length: 6 |
| `rste` | `string` | `varchar` |  |  | Site Incoming Parts. Max length: 9 |
| `whip` | `string` | `varchar` |  |  | Warehouse Incoming Parts. Max length: 6 |
| `lcop` | `string` | `varchar` |  |  | Location Outgoing Parts. Max length: 6 |
| `dste` | `string` | `varchar` |  |  | Site Outgoing Parts. Max length: 9 |
| `whop` | `string` | `varchar` |  |  | Warehouse Outgoing Parts. Max length: 6 |
| `sfsi` | `string` | `varchar` |  |  | Repair Site. Max length: 9 |
| `fwrh` | `string` | `varchar` |  |  | Repair Warehouse. Max length: 6 |
| `cste` | `string` | `varchar` |  |  | Site Return Material. Max length: 9 |
| `cwrh` | `string` | `varchar` |  |  | Warehouse Return Material. Max length: 6 |
| `cdst` | `string` | `varchar` |  |  | Site Delivery Material. Max length: 9 |
| `cdwh` | `string` | `varchar` |  |  | Warehouse Delivery Material. Max length: 6 |
| `mwrh` | `string` | `varchar` |  |  | Material Warehouse. Max length: 6 |
| `pddp` | `string` | `varchar` |  |  | Production Department. Max length: 6 |
| `rdst` | `string` | `varchar` |  |  | Rental Delivery Site. Max length: 9 |
| `rdwh` | `string` | `varchar` |  |  | Rental Delivery Warehouse. Max length: 6 |
| `rrst` | `string` | `varchar` |  |  | Rental Return Site. Max length: 9 |
| `rrwh` | `string` | `varchar` |  |  | Rental Return Warehouse. Max length: 6 |
| `qtim` | `float` | `float` |  |  | Queue Time Work Center |
| `grsb` | `string` | `varchar` |  |  | Number Group Subcontract Agreement. Max length: 3 |
| `snsb` | `string` | `varchar` |  |  | Subcontract Agreement Series Number. Max length: 8 |
| `cbwc` | `integer` | `int` |  |  | Take Cost Rate of Operations Department. Allowed values: 0, 1, 2 |
| `cbwc_kw` | `string` | `varchar` |  |  | Take Cost Rate of Operations Department (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `grmq` | `string` | `varchar` |  |  | Number Group Quotes. Max length: 3 |
| `snmq` | `string` | `varchar` |  |  | Series For Quotes. Max length: 8 |
| `grfq` | `string` | `varchar` |  |  | Number Group for Service Order Quotes. Max length: 3 |
| `snfq` | `string` | `varchar` |  |  | Series for Service Quotes. Max length: 8 |
| `grrq` | `string` | `varchar` |  |  | Number Group for Quote Requests. Max length: 3 |
| `snrq` | `string` | `varchar` |  |  | Quote Requests Series Number. Max length: 8 |
| `grfr` | `string` | `varchar` |  |  | Number Group for Service Order Quote Requests. Max length: 3 |
| `snfr` | `string` | `varchar` |  |  | Series for Service Quote Request. Max length: 8 |
| `grrn` | `string` | `varchar` |  |  | Number Group for Rental Equipment Service Orders. Max length: 3 |
| `srrn` | `string` | `varchar` |  |  | Series for Rental Equipment Service Orders. Max length: 8 |
| `grrr` | `string` | `varchar` |  |  | Number Group for Equipment Rental Requests. Max length: 3 |
| `srrr` | `string` | `varchar` |  |  | Series for Equipment Rental Requests. Max length: 8 |
| `grnq` | `string` | `varchar` |  |  | Number Group for Rental Order Quotes. Max length: 3 |
| `snnq` | `string` | `varchar` |  |  | Series for Rental Order Quotes. Max length: 8 |
| `clrt` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `slrt` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `grcq_sncq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `srep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `grsc_snsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grsq_snsq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grso_snso_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grca_snca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grfc_snfc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grcl_sncl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsc_srsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grwo_snwo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grms_snms_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `lcip_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tswcs025 Locations |
| `rste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `whip_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `lcop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tswcs025 Locations |
| `dste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `whop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `fwrh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwrh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cdst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cdwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `mwrh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `pddp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `rdst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `rdwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `rrst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `rrwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `grsb_snsb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grmq_snmq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grfq_snfq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grrq_snrq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grfr_snfr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grrn_srrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `grrr_srrr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grrr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `grnq_snnq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `grnq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
| `slrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
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
