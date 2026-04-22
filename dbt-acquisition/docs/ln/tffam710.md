# tffam710

LN tffam710 Depreciation Method table, Generated 2026-04-10T19:41:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam710` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `meth` |
| **Column count** | 81 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `meth` | `string` | `varchar` | `PK` | `not_null` | (required) Depreciation Code. Max length: 20 |
| `conv` | `integer` | `int` |  |  | Avg. Convention. Allowed values: 1, 2, 3, 4, 5, 8, 9, 10, 11, 12, 13 |
| `conv_kw` | `string` | `varchar` |  |  | Avg. Convention (keyword). Allowed values: tffam.conv.none, tffam.conv.mid.mont, tffam.conv.mid.quar, tffam.conv.half.year, tffam.conv.modi.half.year, tffam.conv.period.in, tffam.conv.first.period, tffam.conv.next.first.per, tffam.conv.period.after, tffam.conv.first.second, tffam.conv.second.half |
| `dbpt` | `float` | `float` |  |  | Declining Bal. Percent |
| `depl` | `integer` | `int` |  |  | Depreciate Last Year in Service. Allowed values: 1, 2 |
| `depl_kw` | `string` | `varchar` |  |  | Depreciate Last Year in Service (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `face` | `integer` | `int` |  |  | Federal Tax - U.S. (ACE). Allowed values: 0, 1, 2 |
| `face_kw` | `string` | `varchar` |  |  | Federal Tax - U.S. (ACE) (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `famt` | `integer` | `int` |  |  | Federal Tax - U.S. (AMT). Allowed values: 0, 1, 2 |
| `famt_kw` | `string` | `varchar` |  |  | Federal Tax - U.S. (AMT) (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fcal` | `integer` | `int` |  |  | Calculatory. Allowed values: 0, 1, 2 |
| `fcal_kw` | `string` | `varchar` |  |  | Calculatory (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fcom` | `integer` | `int` |  |  | Commercial. Allowed values: 0, 1, 2 |
| `fcom_kw` | `string` | `varchar` |  |  | Commercial (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fixa_1` | `float` | `float` |  |  | Fixed Amount |
| `fixa_2` | `float` | `float` |  |  | Fixed Amount |
| `fixa_3` | `float` | `float` |  |  | Fixed Amount |
| `ffin` | `integer` | `int` |  |  | Financial - U.S.. Allowed values: 0, 1, 2 |
| `ffin_kw` | `string` | `varchar` |  |  | Financial - U.S. (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `foth` | `integer` | `int` |  |  | Other Tax - U.S.. Allowed values: 0, 1, 2 |
| `foth_kw` | `string` | `varchar` |  |  | Other Tax - U.S. (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fspe` | `integer` | `int` |  |  | Special. Allowed values: 0, 1, 2 |
| `fspe_kw` | `string` | `varchar` |  |  | Special (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fsta` | `integer` | `int` |  |  | Statutory. Allowed values: 0, 1, 2 |
| `fsta_kw` | `string` | `varchar` |  |  | Statutory (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fstd` | `integer` | `int` |  |  | Federal Tax - U.S. (Standard). Allowed values: 0, 1, 2 |
| `fstd_kw` | `string` | `varchar` |  |  | Federal Tax - U.S. (Standard) (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `intr` | `float` | `float` |  |  | Interest Rate |
| `life` | `integer` | `int` |  |  | Class Life YY/MM |
| `rtty` | `integer` | `int` |  |  | Rate Table Type. Allowed values: 1, 2, 3 |
| `rtty_kw` | `string` | `varchar` |  |  | Rate Table Type (keyword). Allowed values: tffam.rtty.na, tffam.rtty.month, tffam.rtty.year |
| `swsl` | `integer` | `int` |  |  | Switch to SL. Allowed values: 1, 2 |
| `swsl_kw` | `string` | `varchar` |  |  | Switch to SL (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `taxt` | `integer` | `int` |  |  | Tax Method Type. Allowed values: 1, 2, 3, 5 |
| `taxt_kw` | `string` | `varchar` |  |  | Tax Method Type (keyword). Allowed values: tffam.taxt.na, tffam.taxt.macrs, tffam.taxt.acrs, tffam.taxt.months |
| `type` | `integer` | `int` |  |  | Depreciation Method. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 |
| `type_kw` | `string` | `varchar` |  |  | Depreciation Method (keyword). Allowed values: tffam.mtyp.sl, tffam.mtyp.db, tffam.mtyp.syd, tffam.mtyp.uop, tffam.mtyp.gwg, tffam.mtyp.annuity, tffam.mtyp.fixed, tffam.mtyp.custom, tffam.mtyp.none, tffam.mtyp.nbv, tffam.mtyp.generic, tffam.mtyp.gfac, tffam.mtyp.cost.perc, tffam.mtyp.skloc, tffam.mtyp.czloc |
| `updt` | `integer` | `int` |  |  | Allow Updates. Allowed values: 0, 1, 2 |
| `updt_kw` | `string` | `varchar` |  |  | Allow Updates (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `post` | `integer` | `int` |  |  | Post Reversals. Allowed values: 0, 1, 2 |
| `post_kw` | `string` | `varchar` |  |  | Post Reversals (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `disp` | `integer` | `int` |  |  | Disposal Conventions. Allowed values: 1, 2, 3, 4, 5, 6 |
| `disp_kw` | `string` | `varchar` |  |  | Disposal Conventions (keyword). Allowed values: tffam.disp.none, tffam.disp.disp.per, tffam.disp.prev.per, tffam.disp.first.second, tffam.disp.year.disp, tffam.disp.prev.year |
| `capp` | `integer` | `int` |  |  | Capping. Allowed values: 1, 2, 3, 4 |
| `capp_kw` | `string` | `varchar` |  |  | Capping (keyword). Allowed values: tffam.capp.twice, tffam.capp.triple, tffam.capp.na, tffam.capp.two.and.half |
| `calb` | `integer` | `int` |  |  | Calculation Base. Allowed values: 1, 2, 3 |
| `calb_kw` | `string` | `varchar` |  |  | Calculation Base (keyword). Allowed values: tffam.calb.yearly, tffam.calb.periodic, tffam.calb.na |
| `bnbv` | `integer` | `int` |  |  | Based on Book Value. Allowed values: 1, 2 |
| `bnbv_kw` | `string` | `varchar` |  |  | Based on Book Value (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `didt` | `integer` | `int` |  |  | Include Disposal Date. Allowed values: 1, 2 |
| `didt_kw` | `string` | `varchar` |  |  | Include Disposal Date (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gfac` | `float` | `float` |  |  | Guarantee Factor |
| `rdgf` | `float` | `float` |  |  | Revi Dep Beyond Guarantee Factor |
| `nygf` | `integer` | `int` |  |  | No of Years Beyond Guarantee Factor |
| `dprc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `dprc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `perc` | `float` | `float` |  |  | Obsolete |
| `sbsv` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `sbsv_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bdad` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `bdad_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tffam.bdad.adj.period, tffam.bdad.next.period, tffam.bdad.not.appl |
| `cper` | `float` | `float` |  |  | Cost Percentage |
| `stcp` | `integer` | `int` |  |  | Switch to Cost Percentage. Allowed values: 1, 2 |
| `stcp_kw` | `string` | `varchar` |  |  | Switch to Cost Percentage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acnp` | `integer` | `int` |  |  | Apply Cost Adjustment in next Period. Allowed values: 1, 2 |
| `acnp_kw` | `string` | `varchar` |  |  | Apply Cost Adjustment in next Period (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `silp` | `integer` | `int` |  |  | Salvage in last Period. Allowed values: 1, 2 |
| `silp_kw` | `string` | `varchar` |  |  | Salvage in last Period (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rndy` | `integer` | `int` |  |  | Round Year Amount. Allowed values: 1, 2 |
| `rndy_kw` | `string` | `varchar` |  |  | Round Year Amount (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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
