# qmptc008

LN qmptc008 Instruments table, Generated 2022-06-15T01:21:04Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc008` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `inst`, `inno` |
| **Column count** | 36 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `inst` | `string` | `varchar` | `PK` | `not_null` | (required) Instrument. Max length: 6 |
| `inno` | `string` | `varchar` | `PK` | `not_null` | (required) Instrument Number. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `intg` | `string` | `varchar` |  |  | Instrument Group. Max length: 6 |
| `tare` | `string` | `varchar` |  |  | Test Area. Max length: 3 |
| `skll` | `string` | `varchar` |  |  | Skill. Max length: 9 |
| `lcnt` | `integer` | `int` |  |  | Least Measurable Quantity |
| `culc` | `string` | `varchar` |  |  | Least Measurable Unit. Max length: 3 |
| `cbit` | `integer` | `int` |  |  | Calibration Interval Type. Allowed values: 1, 2, 3 |
| `cbit_kw` | `string` | `varchar` |  |  | Calibration Interval Type (keyword). Allowed values: qmptc.cbit.not.appl, qmptc.cbit.time, qmptc.cbit.usage |
| `cbin` | `integer` | `int` |  |  | Calibration Interval [Days/Times Used] |
| `lcdt` | `timestamp` | `timestamp_ntz` |  |  | Last Calibration Date |
| `ncdt` | `timestamp` | `timestamp_ntz` |  |  | Next Calibration Date |
| `ncus` | `integer` | `int` |  |  | Next Calibration [Times Used] |
| `bcal` | `integer` | `int` |  |  | Blocked for Calibration. Allowed values: 1, 2 |
| `bcal_kw` | `string` | `varchar` |  |  | Blocked for Calibration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ausg` | `integer` | `int` |  |  | Actual Times Used |
| `cbdt` | `timestamp` | `timestamp_ntz` |  |  | Calibration Date |
| `txta` | `integer` | `int` |  |  | Instrument Text |
| `txtb` | `integer` | `int` |  |  | Calibration Text |
| `intg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc024 Instrument Group |
| `tare_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc007 Test Areas |
| `skll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl010 Skills |
| `culc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
