# tdpur200

LN tdpur200 Purchase Requisitions table, Generated 2026-04-10T19:41:19Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rqno` |
| **Column count** | 64 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rqno` | `string` | `varchar` | `PK` | `not_null` | (required) Requisition. Max length: 9 |
| `orig` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 29, 30, 99 |
| `orig_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sls.schedule, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.subc.pur.sched, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit, tdpur.corg.price.calc, tdpur.corg.not.appl |
| `remn` | `string` | `varchar` |  |  | Requester. Max length: 9 |
| `rdep` | `string` | `varchar` |  |  | Requester Department. Max length: 6 |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Requisition Date |
| `aemn` | `string` | `varchar` |  |  | Approver. Max length: 9 |
| `adep` | `string` | `varchar` |  |  | Approver Department. Max length: 6 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Latest Transaction Date |
| `rqst` | `integer` | `int` |  |  | Requisition Status. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `rqst_kw` | `string` | `varchar` |  |  | Requisition Status (keyword). Allowed values: tdpur.rqst.created, tdpur.rqst.submitted, tdpur.rqst.approved, tdpur.rqst.rejected, tdpur.rqst.modified, tdpur.rqst.canceled, tdpur.rqst.converted, tdpur.rqst.closed, tdpur.rqst.in.process |
| `conv` | `integer` | `int` |  |  | Allow Partial Rejection. Allowed values: 1, 2 |
| `conv_kw` | `string` | `varchar` |  |  | Allow Partial Rejection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `dadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Requested Date |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `logn` | `string` | `varchar` |  |  | Login Code. Max length: 16 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `urgt` | `integer` | `int` |  |  | Urgent. Allowed values: 1, 2 |
| `urgt_kw` | `string` | `varchar` |  |  | Urgent (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnty` | `integer` | `int` |  |  | Conversion Type. Allowed values: 1, 2, 3 |
| `cnty_kw` | `string` | `varchar` |  |  | Conversion Type (keyword). Allowed values: tdpur.cnty.none, tdpur.cnty.rfq, tdpur.cnty.pur |
| `spap` | `integer` | `int` |  |  | Spend Approved. Allowed values: 1, 2 |
| `spap_kw` | `string` | `varchar` |  |  | Spend Approved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcod` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `dral` | `integer` | `int` |  |  | Default Receipt Address for All Lines. Allowed values: 1, 2 |
| `dral_kw` | `string` | `varchar` |  |  | Default Receipt Address for All Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Text |
| `remn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `rdep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `aemn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `adep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `dadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
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
