# tffam110

LN tffam110 Asset Book table, Generated 2026-04-10T19:41:32Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `anbr`, `aext`, `book` |
| **Column count** | 104 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `anbr` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Extension. Max length: 15 |
| `book` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Book. Max length: 10 |
| `adpa_1` | `float` | `float` |  |  | Additional Posting Amount |
| `adpa_2` | `float` | `float` |  |  | Additional Posting Amount |
| `adpa_3` | `float` | `float` |  |  | Additional Posting Amount |
| `adpc` | `integer` | `int` |  |  | Additional Posting. Allowed values: 0, 1, 2 |
| `adpc_kw` | `string` | `varchar` |  |  | Additional Posting (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `adpm` | `string` | `varchar` |  |  | Depreciation Code. Max length: 20 |
| `bslv` | `integer` | `int` |  |  | Depreciate Below Salvage. Allowed values: 1, 2 |
| `bslv_kw` | `string` | `varchar` |  |  | Depreciate Below Salvage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `c263_1` | `float` | `float` |  |  | Allocated Cost |
| `c263_2` | `float` | `float` |  |  | Allocated Cost |
| `c263_3` | `float` | `float` |  |  | Allocated Cost |
| `capi_1` | `float` | `float` |  |  | Capitalized Interest |
| `capi_2` | `float` | `float` |  |  | Capitalized Interest |
| `capi_3` | `float` | `float` |  |  | Capitalized Interest |
| `cost_1` | `float` | `float` |  |  | Current Cost |
| `cost_2` | `float` | `float` |  |  | Current Cost |
| `cost_3` | `float` | `float` |  |  | Current Cost |
| `curd` | `date` | `date` |  |  | Recovery Date |
| `depr_1` | `float` | `float` |  |  | Depreciation Amount |
| `depr_2` | `float` | `float` |  |  | Depreciation Amount |
| `depr_3` | `float` | `float` |  |  | Depreciation Amount |
| `disd` | `date` | `date` |  |  | Final Disposal Date |
| `ecre` | `integer` | `int` |  |  | Economic Recapture. Allowed values: 1, 2 |
| `ecre_kw` | `string` | `varchar` |  |  | Economic Recapture (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `freq` | `string` | `varchar` |  |  | Depreciation Frequency. Max length: 10 |
| `indx` | `string` | `varchar` |  |  | Revaluation Index. Max length: 3 |
| `itca_1` | `float` | `float` |  |  | Current ITC Amount |
| `itca_2` | `float` | `float` |  |  | Current ITC Amount |
| `itca_3` | `float` | `float` |  |  | Current ITC Amount |
| `last` | `date` | `date` |  |  | Last Depreciation Date |
| `life` | `integer` | `int` |  |  | Asset Life |
| `lrev` | `integer` | `int` |  |  | Last Revaluation Year |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `meth` | `string` | `varchar` |  |  | Depreciation Code. Max length: 20 |
| `ocst_1` | `float` | `float` |  |  | Original Cost |
| `ocst_2` | `float` | `float` |  |  | Original Cost |
| `ocst_3` | `float` | `float` |  |  | Original Cost |
| `post` | `integer` | `int` |  |  | Depreciation Postings to General Ledger. Allowed values: 1, 2 |
| `post_kw` | `string` | `varchar` |  |  | Depreciation Postings to General Ledger (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prop` | `string` | `varchar` |  |  | Property Type. Max length: 10 |
| `s179_1` | `float` | `float` |  |  | Section 179 Value |
| `s179_2` | `float` | `float` |  |  | Section 179 Value |
| `s179_3` | `float` | `float` |  |  | Section 179 Value |
| `salv_1` | `float` | `float` |  |  | Salvage Value |
| `salv_2` | `float` | `float` |  |  | Salvage Value |
| `salv_3` | `float` | `float` |  |  | Salvage Value |
| `shft` | `float` | `float` |  |  | Shift Factor |
| `stat` | `integer` | `int` |  |  | Asset Book Status. Allowed values: 1, 2, 3, 4, 5 |
| `stat_kw` | `string` | `varchar` |  |  | Asset Book Status (keyword). Allowed values: tffam.bsta.ente, tffam.bsta.capi, tffam.bsta.depr, tffam.bsta.disp, tffam.bsta.full.depr |
| `tmth` | `integer` | `int` |  |  | MACRS Depreciation By. Allowed values: 0, 1, 2 |
| `tmth_kw` | `string` | `varchar` |  |  | MACRS Depreciation By (keyword). Allowed values: , tffam.tmth.tabl, tffam.tmth.form |
| `unit` | `integer` | `int` |  |  | Units |
| `ytdd_1` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_2` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_3` | `float` | `float` |  |  | YTD Depreciation |
| `ytdr_1` | `float` | `float` |  |  | YTD Revaluation |
| `ytdr_2` | `float` | `float` |  |  | YTD Revaluation |
| `ytdr_3` | `float` | `float` |  |  | YTD Revaluation |
| `sdpn` | `integer` | `int` |  |  | Count of Suspended Periods |
| `ltad_1` | `float` | `float` |  |  | Accumulated Accelerated Depreciation |
| `ltad_2` | `float` | `float` |  |  | Accumulated Accelerated Depreciation |
| `ltad_3` | `float` | `float` |  |  | Accumulated Accelerated Depreciation |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `srac` | `integer` | `int` |  |  | Separate Revaluation Accounting. Allowed values: 1, 2 |
| `srac_kw` | `string` | `varchar` |  |  | Separate Revaluation Accounting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rdpr_1` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_2` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_3` | `float` | `float` |  |  | Revaluation Depreciation |
| `ltdr_1` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_2` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_3` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltar_1` | `float` | `float` |  |  | Accumulated Accelerated Revaluation Depreciation |
| `ltar_2` | `float` | `float` |  |  | Accumulated Accelerated Revaluation Depreciation |
| `ltar_3` | `float` | `float` |  |  | Accumulated Accelerated Revaluation Depreciation |
| `gfac_1` | `float` | `float` |  |  | Guarantee Value |
| `gfac_2` | `float` | `float` |  |  | Guarantee Value |
| `gfac_3` | `float` | `float` |  |  | Guarantee Value |
| `fydp` | `float` | `float` |  |  | First Year Depreciation Percentage |
| `txta` | `integer` | `int` |  |  | Comment |
| `anbr_aext_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam100 Asset |
| `book_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam600 Books |
| `adpm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `freq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam700 Depreciation Frequency |
| `indx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam300 Index Master Data |
| `meth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `prop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam780 Property Type |
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
