# tcmcs013

LN tcmcs013 Payment Terms table, Generated 2026-04-10T19:41:10Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs013` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpay` |
| **Column count** | 51 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpay` | `string` | `varchar` | `PK` | `not_null` | (required) Payment Terms. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `pper` | `integer` | `int` |  |  | Payment Period |
| `pash` | `string` | `varchar` |  |  | Payment Schedule Header. Max length: 3 |
| `disa` | `integer` | `int` |  |  | Discount Period 1 |
| `disb` | `integer` | `int` |  |  | Discount Period 2 |
| `disc` | `integer` | `int` |  |  | Discount Period 3 |
| `prca` | `float` | `float` |  |  | Discount Percentage 1 |
| `prcb` | `float` | `float` |  |  | Discount Percentage 2 |
| `prcc` | `float` | `float` |  |  | Discount Percentage 3 |
| `atie` | `integer` | `int` |  |  | Discount Including Tax. Allowed values: 1, 2 |
| `atie_kw` | `string` | `varchar` |  |  | Discount Including Tax (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pdin` | `integer` | `int` |  |  | Tax After Discount. Allowed values: 1, 2 |
| `pdin_kw` | `string` | `varchar` |  |  | Tax After Discount (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdde` | `integer` | `int` |  |  | Due Date Calculation Method. Allowed values: 1, 2, 3, 4, 5, 100 |
| `cdde_kw` | `string` | `varchar` |  |  | Due Date Calculation Method (keyword). Allowed values: tccdde.direct, tccdde.end.month, tccdde.end.fortnight, tccdde.end.ten.days, tccdde.end.week, tccdde.not.appl |
| `cdis` | `integer` | `int` |  |  | Discount Date Calculation Method. Allowed values: 1, 2, 3, 4, 5, 100 |
| `cdis_kw` | `string` | `varchar` |  |  | Discount Date Calculation Method (keyword). Allowed values: tccdde.direct, tccdde.end.month, tccdde.end.fortnight, tccdde.end.ten.days, tccdde.end.week, tccdde.not.appl |
| `fdue` | `integer` | `int` |  |  | Fence for Due Date |
| `fdis` | `integer` | `int` |  |  | Fence for Discount Date |
| `told` | `float` | `float` |  |  | Tolerance for Discount Percentage |
| `tola` | `float` | `float` |  |  | Tolerance for Discount Amount |
| `tolp` | `integer` | `int` |  |  | Tolerance for Discount Period |
| `tlsd` | `integer` | `int` |  |  | Tolerance for Shift in Due Date |
| `day1` | `integer` | `int` |  |  | Payment Day 1 |
| `day2` | `integer` | `int` |  |  | Payment Day 2 |
| `day3` | `integer` | `int` |  |  | Payment Day 3 |
| `ptyp` | `integer` | `int` |  |  | Payment Period Type. Allowed values: 1, 2, 3 |
| `ptyp_kw` | `string` | `varchar` |  |  | Payment Period Type (keyword). Allowed values: tcacp.ptyp.days, tcacp.ptyp.months, tcacp.ptyp.periods |
| `prio` | `integer` | `int` |  |  | Priority for Due Date Calc.. Allowed values: 1, 2, 3 |
| `prio_kw` | `string` | `varchar` |  |  | Priority for Due Date Calc. (keyword). Allowed values: tcmcs.prio.not.app, tcmcs.prio.end.month, tcmcs.prio.pay.period |
| `pdyn` | `integer` | `int` |  |  | Use Payment Days for Discount Calculation. Allowed values: 1, 2 |
| `pdyn_kw` | `string` | `varchar` |  |  | Use Payment Days for Discount Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pdis` | `integer` | `int` |  |  | Discount Date Calculation Priority. Allowed values: 1, 2, 3 |
| `pdis_kw` | `string` | `varchar` |  |  | Discount Date Calculation Priority (keyword). Allowed values: tcmcs.prio.not.app, tcmcs.prio.end.month, tcmcs.prio.pay.period |
| `dtbs` | `integer` | `int` |  |  | Distribute Tax Based on Schedule. Allowed values: 1, 2 |
| `dtbs_kw` | `string` | `varchar` |  |  | Distribute Tax Based on Schedule (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vtpm` | `string` | `varchar` |  |  | VAT Payment Method. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Payment Terms Text |
| `pash_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs220 Payment Schedule Headers |
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
