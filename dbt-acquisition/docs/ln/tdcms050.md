# tdcms050

LN tdcms050 Commissions/Rebates table, Generated 2026-04-10T19:41:16Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdcms050` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `type`, `orno`, `pono`, `sqnb`, `dsqn`, `invl`, `reln`, `sern` |
| **Column count** | 85 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `type` | `integer` | `int` | `PK` | `not_null` | (required) Type. Allowed values: 1, 2 |
| `type_kw` | `string` | `varchar` |  |  | Type (keyword). Allowed values: tdcms.type.commission, tdcms.type.rebate |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Sales Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `dsqn` | `integer` | `int` | `PK` | `not_null` | (required) Actual Delivery Line Sequence Number |
| `invl` | `integer` | `int` | `PK` | `not_null` | (required) Invoice Line |
| `reln` | `string` | `varchar` | `PK` | `not_null` | (required) Relation. Max length: 9 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Serial Number |
| `rlsq` | `integer` | `int` |  |  | Relation Sequence |
| `cmpr` | `float` | `float` |  |  | Commission/Rebate Percentage |
| `cpdt` | `string` | `varchar` |  |  | Period Table. Max length: 6 |
| `year` | `integer` | `int` |  |  | Statistical Year |
| `perd` | `integer` | `int` |  |  | Period |
| `grpc` | `float` | `float` |  |  | Growing Percentage |
| `amnt` | `float` | `float` |  |  | Amount |
| `amta` | `float` | `float` |  |  | Fixed Agreement Amount in Sales Order Currency |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Comm./Rebt Calculation Date |
| `cmam_1` | `float` | `float` |  |  | Commission/Rebate Amount |
| `cmam_2` | `float` | `float` |  |  | Commission/Rebate Amount |
| `cmam_3` | `float` | `float` |  |  | Commission/Rebate Amount |
| `cmst` | `integer` | `int` |  |  | Comm./Rebt Status. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `cmst_kw` | `string` | `varchar` |  |  | Comm./Rebt Status (keyword). Allowed values: tdcms.cmst.calculated, tdcms.cmst.manual, tdcms.cmst.approved, tdcms.cmst.reserved, tdcms.cmst.ready, tdcms.cmst.closed, tdcms.cmst.reserv.approved |
| `invn` | `integer` | `int` |  |  | Invoice |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `ttyp` | `string` | `varchar` |  |  | Sales Invoice Transaction Type. Max length: 3 |
| `prty` | `string` | `varchar` |  |  | Search Priority. Max length: 6 |
| `crtt` | `string` | `varchar` |  |  | Comm./Rebt Transaction Type Invoice. Max length: 3 |
| `crin` | `integer` | `int` |  |  | Comm./Rebt Invoice |
| `resv` | `integer` | `int` |  |  | Reserve. Allowed values: 1, 2 |
| `resv_kw` | `string` | `varchar` |  |  | Reserve (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crtp` | `string` | `varchar` |  |  | Discount. Max length: 3 |
| `trdt` | `date` | `date` |  |  | Reservation Date |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `fdpt` | `string` | `varchar` |  |  | Financial Department. Max length: 6 |
| `reld` | `integer` | `int` |  |  | Released to SLI(Rebate). Allowed values: 1, 2 |
| `reld_kw` | `string` | `varchar` |  |  | Released to SLI(Rebate) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Printed Date |
| `upid` | `integer` | `int` |  |  | Process ID |
| `ccur` | `string` | `varchar` |  |  | Sales Order Currency. Max length: 3 |
| `icur` | `string` | `varchar` |  |  | Comm./Rebt. Invoice Currency. Max length: 3 |
| `amoc` | `float` | `float` |  |  | Amount in Sales Order Currency |
| `amic` | `float` | `float` |  |  | Amount in Comm./Rebt. Invoice Currency |
| `orat_1` | `float` | `float` |  |  | Sales Order Rate |
| `orat_2` | `float` | `float` |  |  | Sales Order Rate |
| `orat_3` | `float` | `float` |  |  | Sales Order Rate |
| `oraf_1` | `integer` | `int` |  |  | Sales Order Rate Factor |
| `oraf_2` | `integer` | `int` |  |  | Sales Order Rate Factor |
| `oraf_3` | `integer` | `int` |  |  | Sales Order Rate Factor |
| `ordt` | `timestamp` | `timestamp_ntz` |  |  | Sales Order Rate Date |
| `ortp` | `string` | `varchar` |  |  | Sales Order Rate Type. Max length: 3 |
| `irat_1` | `float` | `float` |  |  | Sales Invoice Rate |
| `irat_2` | `float` | `float` |  |  | Sales Invoice Rate |
| `irat_3` | `float` | `float` |  |  | Sales Invoice Rate |
| `iraf_1` | `integer` | `int` |  |  | Sales Invoice Rate Factor |
| `iraf_2` | `integer` | `int` |  |  | Sales Invoice Rate Factor |
| `iraf_3` | `integer` | `int` |  |  | Sales Invoice Rate Factor |
| `irdt` | `timestamp` | `timestamp_ntz` |  |  | Sales Invoice Rate Date |
| `irtp` | `string` | `varchar` |  |  | Sales Invoice Rate Type. Max length: 3 |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls400 Sales Orders |
| `reln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdcms010 Relations |
| `cpdt_year_perd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cpdt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `crtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs021 Discount Codes |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `fdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `icur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ortp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `irtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cmam_trnc` | `float` | `float` |  |  | CC: Commission/Rebate Amount in Transaction Currency |
| `cmam_rpc1` | `float` | `float` |  |  | CC: Commission/Rebate Amount in Reporting Currency 1 |
| `cmam_rpc2` | `float` | `float` |  |  | CC: Commission/Rebate Amount in Reporting Currency 2 |
| `cmam_rfrc` | `float` | `float` |  |  | CC: Commission/Rebate Amount in Reference Currency |
| `cmam_dtwc` | `float` | `float` |  |  | CC: Commission/Rebate Amount in Data Warehouse Currency |
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
