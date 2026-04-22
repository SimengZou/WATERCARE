# qmptc115

LN qmptc115 Inspection Order Test Data table, Generated 2022-06-15T01:21:06Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc115` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `iorn`, `saml`, `pono`, `srno` |
| **Column count** | 55 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `iorn` | `string` | `varchar` | `PK` | `not_null` | (required) Inspection Order. Max length: 9 |
| `saml` | `integer` | `int` | `PK` | `not_null` | (required) Sample |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Inspection Order Line |
| `srno` | `integer` | `int` | `PK` | `not_null` | (required) Sample Part |
| `tseq` | `integer` | `int` |  |  | Test Sequence |
| `tsdt` | `timestamp` | `timestamp_ntz` |  |  | Test Time |
| `mval` | `float` | `float` |  |  | Measurement Value |
| `oset` | `string` | `varchar` |  |  | Option Set. Max length: 6 |
| `mopt` | `string` | `varchar` |  |  | Option. Max length: 8 |
| `tare` | `string` | `varchar` |  |  | Test Area. Max length: 3 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `inst` | `string` | `varchar` |  |  | Instrument. Max length: 6 |
| `inno` | `string` | `varchar` |  |  | Instrument Number. Max length: 9 |
| `tqua` | `float` | `float` |  |  | Test Quantity |
| `tuni` | `string` | `varchar` |  |  | Test Unit. Max length: 3 |
| `resl` | `integer` | `int` |  |  | Result. Allowed values: 0, 1, 2, 3 |
| `resl_kw` | `string` | `varchar` |  |  | Result (keyword). Allowed values: , qmptc.resl.good, qmptc.resl.bad, qmptc.resl.not.inspected |
| `ncmr` | `string` | `varchar` |  |  | Non-Conforming Material Report. Max length: 9 |
| `aemn` | `string` | `varchar` |  |  | Actual Employee. Max length: 9 |
| `etim` | `float` | `float` |  |  | Estimated Time |
| `aemp` | `string` | `varchar` |  |  | Actual Employee. Max length: 16 |
| `text` | `integer` | `int` |  |  | Text |
| `iorn_saml_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc110 Inspection Order Samples |
| `iorn_pono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc101 Inspection Order Lines |
| `iorn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc100 Inspection Orders |
| `oset_mopt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc014 Options |
| `tare_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc007 Test Areas |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `inst_inno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc008 Instruments |
| `tuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ncmr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm100 Non-Conformance Report (NCR) |
| `aemn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `mval_buar` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Area |
| `mval_buln` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Length |
| `mval_bupc` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Piece |
| `mval_butm` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Time |
| `mval_buvl` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Volume |
| `mval_buwg` | `float` | `float` |  |  | CC: Sample Quantity in Base Unit Weight |
| `tqua_buar` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Area |
| `tqua_buln` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Length |
| `tqua_bupc` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Piece |
| `tqua_butm` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Time |
| `tqua_buvl` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Volume |
| `tqua_buwg` | `float` | `float` |  |  | CC: Test Quantity in Base Unit Weight |
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
