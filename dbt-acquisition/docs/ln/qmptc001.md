# qmptc001

LN qmptc001 Characteristics table, Generated 2022-06-15T01:21:04Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `char` |
| **Column count** | 53 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `char` | `string` | `varchar` | `PK` | `not_null` | (required) Characteristic. Max length: 8 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `ctyp` | `integer` | `int` |  |  | Characteristic Type. Allowed values: 1, 2, 3 |
| `ctyp_kw` | `string` | `varchar` |  |  | Characteristic Type (keyword). Allowed values: qmptc.ctyp.fraction, qmptc.ctyp.integer, qmptc.ctyp.option |
| `mthd` | `integer` | `int` |  |  | Method. Allowed values: 1, 2, 3 |
| `mthd_kw` | `string` | `varchar` |  |  | Method (keyword). Allowed values: qmptc.mthd.fixed, qmptc.mthd.variable, qmptc.mthd.algorithm |
| `algo` | `string` | `varchar` |  |  | Algorithm. Max length: 6 |
| `cstd__en_us` | `string` | `varchar` |  | `not_null` | (required) Characteristic Standard - base language. Max length: 8 |
| `dtst` | `string` | `varchar` |  |  | Default Test. Max length: 8 |
| `chun` | `string` | `varchar` |  |  | Characteristic Unit. Max length: 3 |
| `fcvl` | `float` | `float` |  |  | Fixed Characteristic Value |
| `falg` | `integer` | `int` |  |  | For Algorithm. Allowed values: 0, 1, 2 |
| `falg_kw` | `string` | `varchar` |  |  | For Algorithm (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `oset` | `string` | `varchar` |  |  | Option Set. Max length: 6 |
| `cusg` | `integer` | `int` |  |  | Characteristic Usage. Allowed values: 10, 20 |
| `cusg_kw` | `string` | `varchar` |  |  | Characteristic Usage (keyword). Allowed values: qmptc.usag.anal, qmptc.usag.var |
| `vaty` | `integer` | `int` |  |  | Value Type. Allowed values: 1, 2, 3 |
| `vaty_kw` | `string` | `varchar` |  |  | Value Type (keyword). Allowed values: qmptc.vaty.norm, qmptc.vaty.nominal, qmptc.vaty.not.appl |
| `kolt` | `integer` | `int` |  |  | Limit Type. Allowed values: 1, 2, 3 |
| `kolt_kw` | `string` | `varchar` |  |  | Limit Type (keyword). Allowed values: qmptc.kolt.value, qmptc.kolt.tolerance, qmptc.kolt.not.appl |
| `norm` | `float` | `float` |  |  | Norm |
| `chrt` | `string` | `varchar` |  |  | Chart Name. Max length: 10 |
| `chty` | `string` | `varchar` |  |  | Chart Type. Max length: 10 |
| `tost` | `string` | `varchar` |  |  | Tolerance Standard. Max length: 10 |
| `nomi` | `float` | `float` |  |  | Nominal Value |
| `ulmt` | `float` | `float` |  |  | Upper Limit |
| `llmt` | `float` | `float` |  |  | Lower Limit |
| `utln` | `float` | `float` |  |  | Upper Tolerance |
| `ltln` | `float` | `float` |  |  | Lower Tolerance |
| `utwa` | `float` | `float` |  |  | Upper Warning |
| `ltwa` | `float` | `float` |  |  | Lower Warning |
| `utlw` | `float` | `float` |  |  | Upper Tolerance Warning |
| `ltlw` | `float` | `float` |  |  | Lower Tolerance Warning |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Characteristic Text |
| `algo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc021 Algorithms |
| `dtst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc006 Tests |
| `chun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `oset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc013 Option Sets |
| `chrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc081 Chart Name |
| `chty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc082 Chart Type |
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
