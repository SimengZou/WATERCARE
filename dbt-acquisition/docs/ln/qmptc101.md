# qmptc101

LN qmptc101 Inspection Order Lines table, Generated 2022-06-15T01:21:05Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc101` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `iorn`, `pono` |
| **Column count** | 89 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `iorn` | `string` | `varchar` | `PK` | `not_null` | (required) Inspection Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Inspection Line |
| `aspt` | `string` | `varchar` |  |  | Aspect. Max length: 8 |
| `char` | `string` | `varchar` |  |  | Characteristic. Max length: 8 |
| `docm` | `string` | `varchar` |  |  | Document. Max length: 9 |
| `refr` | `string` | `varchar` |  |  | Reference Designator. Max length: 16 |
| `dfcl` | `integer` | `int` |  |  | Defect Classification. Allowed values: 10, 20, 30, 40 |
| `dfcl_kw` | `string` | `varchar` |  |  | Defect Classification (keyword). Allowed values: qmptc.dfcl.crit, qmptc.dfcl.maj, qmptc.dfcl.min, qmptc.dfcl.na |
| `ctyp` | `integer` | `int` |  |  | Characteristic Type. Allowed values: 1, 2, 3 |
| `ctyp_kw` | `string` | `varchar` |  |  | Characteristic Type (keyword). Allowed values: qmptc.ctyp.fraction, qmptc.ctyp.integer, qmptc.ctyp.option |
| `chun` | `string` | `varchar` |  |  | Characteristic Unit. Max length: 3 |
| `falg` | `integer` | `int` |  |  | For Algorithm. Allowed values: 1, 2 |
| `falg_kw` | `string` | `varchar` |  |  | For Algorithm (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oset` | `string` | `varchar` |  |  | Option Set. Max length: 6 |
| `mthd` | `integer` | `int` |  |  | Method. Allowed values: 1, 2, 3 |
| `mthd_kw` | `string` | `varchar` |  |  | Method (keyword). Allowed values: qmptc.mthd.fixed, qmptc.mthd.variable, qmptc.mthd.algorithm |
| `algo` | `string` | `varchar` |  |  | Algorithm. Max length: 6 |
| `asdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Completion Date |
| `test` | `string` | `varchar` |  |  | Test. Max length: 8 |
| `tare` | `string` | `varchar` |  |  | Test Area. Max length: 3 |
| `dstr` | `integer` | `int` |  |  | Destructive. Allowed values: 1, 2 |
| `dstr_kw` | `string` | `varchar` |  |  | Destructive (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rslt` | `integer` | `int` |  |  | Result Type. Allowed values: 1, 2 |
| `rslt_kw` | `string` | `varchar` |  |  | Result Type (keyword). Allowed values: qmptc.rslt.quantitative, qmptc.rslt.qualitative |
| `tuni` | `string` | `varchar` |  |  | Test Unit. Max length: 3 |
| `tstd__en_us` | `string` | `varchar` |  | `not_null` | (required) Test Standard - base language. Max length: 8 |
| `skll` | `string` | `varchar` |  |  | Skill. Max length: 9 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `intg` | `string` | `varchar` |  |  | Instrument Group. Max length: 6 |
| `inst` | `string` | `varchar` |  |  | Instrument. Max length: 6 |
| `inno` | `string` | `varchar` |  |  | Instrument Number. Max length: 9 |
| `fcvl` | `float` | `float` |  |  | Fixed Characteristic Value |
| `cstd__en_us` | `string` | `varchar` |  | `not_null` | (required) Characteristic Standard - base language. Max length: 8 |
| `norm` | `float` | `float` |  |  | Norm |
| `llmt` | `float` | `float` |  |  | Lower Limit |
| `ulmt` | `float` | `float` |  |  | Upper Limit |
| `cdch` | `string` | `varchar` |  |  | Characteristic Drawing. Max length: 16 |
| `cdtt` | `string` | `varchar` |  |  | Test Drawing. Max length: 16 |
| `cvtu` | `float` | `float` |  |  | Conversion Factor Test Unit |
| `osta` | `integer` | `int` |  |  | Order Line Status. Allowed values: 1, 2, 3, 4, 5 |
| `osta_kw` | `string` | `varchar` |  |  | Order Line Status (keyword). Allowed values: qmptc.osta.free, qmptc.osta.printed, qmptc.osta.active, qmptc.osta.completed, qmptc.osta.cancelled |
| `acre` | `integer` | `int` |  |  | Result. Allowed values: 10, 15, 20, 30 |
| `acre_kw` | `string` | `varchar` |  |  | Result (keyword). Allowed values: qmptc.acre.accept, qmptc.acre.part.accept, qmptc.acre.reject, qmptc.acre.undefined |
| `ltwa` | `float` | `float` |  |  | Lower Warning |
| `utwa` | `float` | `float` |  |  | Upper Warning |
| `vaty` | `integer` | `int` |  |  | Value Type. Allowed values: 1, 2, 3 |
| `vaty_kw` | `string` | `varchar` |  |  | Value Type (keyword). Allowed values: qmptc.vaty.norm, qmptc.vaty.nominal, qmptc.vaty.not.appl |
| `chrt` | `string` | `varchar` |  |  | Chart Name. Max length: 10 |
| `chty` | `string` | `varchar` |  |  | Chart Type. Max length: 10 |
| `tost` | `string` | `varchar` |  |  | Tolerance Standard. Max length: 10 |
| `nomi` | `float` | `float` |  |  | Nominal Value |
| `psdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Date |
| `pcdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Completion Date |
| `iskl` | `string` | `varchar` |  |  | Instrument Skill. Max length: 9 |
| `tipe` | `float` | `float` |  |  | Time Estimate |
| `tiun` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Inspection Order Line Text |
| `iorn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc100 Inspection Orders |
| `aspt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc002 Aspects |
| `aspt_char_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc003 Aspect Characteristics |
| `char_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc001 Characteristics |
| `refr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs090 Reference Designators |
| `chun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `oset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc013 Option Sets |
| `algo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc021 Algorithms |
| `test_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc006 Tests |
| `tare_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc007 Test Areas |
| `tuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `skll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl010 Skills |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `intg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc024 Instrument Group |
| `inst_inno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc008 Instruments |
| `chrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc081 Chart Name |
| `chrt_chty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc085 Nominal Table |
| `chty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc082 Chart Type |
| `iskl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl010 Skills |
| `tiun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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
