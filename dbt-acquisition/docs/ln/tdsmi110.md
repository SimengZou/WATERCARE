# tdsmi110

LN tdsmi110 Opportunities table, Generated 2026-04-10T19:41:27Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsmi110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `opty` |
| **Column count** | 64 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `opty` | `string` | `varchar` | `PK` | `not_null` | (required) Opportunity. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seri` | `string` | `varchar` |  |  | Series. Max length: 8 |
| `bpid` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `crep` | `string` | `varchar` |  |  | Assigned to. Max length: 9 |
| `cptp` | `string` | `varchar` |  |  | Opportunity Type. Max length: 3 |
| `csou` | `string` | `varchar` |  |  | Source. Max length: 3 |
| `opst` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 40, 50, 60 |
| `opst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdsmi.opst.preliminary, tdsmi.opst.in.process, tdsmi.opst.dead, tdsmi.opst.canceled, tdsmi.opst.won, tdsmi.opst.lost |
| `sapr` | `string` | `varchar` |  |  | Sales Process. Max length: 3 |
| `cpha` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `sccp` | `float` | `float` |  |  | Probability Percentage |
| `cdis` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `incf` | `integer` | `int` |  |  | Include in Forecast. Allowed values: 1, 2 |
| `incf_kw` | `string` | `varchar` |  |  | Include in Forecast (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `xpam` | `float` | `float` |  |  | Expected Revenue |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `fcdt` | `timestamp` | `timestamp_ntz` |  |  | Date of First Contact |
| `dcdt` | `timestamp` | `timestamp_ntz` |  |  | Expected Close Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Close Date |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `catt` | `string` | `varchar` |  |  | Attention. Max length: 3 |
| `ccor` | `string` | `varchar` |  |  | Postal Address. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Telephone. Max length: 32 |
| `tefx` | `string` | `varchar` |  |  | Fax. Max length: 32 |
| `guid` | `string` | `varchar` |  |  | Guid. Max length: 22 |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Last Transaction Date |
| `name__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 35 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `mprj` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `role` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `role_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdsmi.role.general, tdsmi.role.soldto, tdsmi.role.shipto, tdsmi.role.invoiceto, tdsmi.role.payto, tdsmi.role.payfrom, tdsmi.role.buyfrom, tdsmi.role.shipfrom, tdsmi.role.invoicefrom |
| `text` | `integer` | `int` |  |  | Opportunity Text |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cptp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsmi007 Opportunity Types |
| `csou_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs124 Sources |
| `sapr_cpha_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsmi009 Phases by Sales Process |
| `sapr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsmi008 Sales Processes |
| `cpha_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsmi003 Phases |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `catt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs122 Attention |
| `ccor_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `xpam_lclc` | `float` | `float` |  |  | CC: Expected Revenue in Local Currency |
| `xpam_rpc1` | `float` | `float` |  |  | CC: Expected Revenue in Reporting Currency 1 |
| `xpam_rpc2` | `float` | `float` |  |  | CC: Expected Revenue in Reporting Currency 2 |
| `xpam_rfrc` | `float` | `float` |  |  | CC: Expected Revenue in Reference Currency |
| `xpam_dtwc` | `float` | `float` |  |  | CC: Expected Revenue in Data Warehouse Currency |
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
