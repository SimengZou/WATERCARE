# tsctm120

LN tsctm120 Coverage Terms table, Generated 2026-04-10T19:42:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm120` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln`, `cctp`, `cotp`, `nrbt` |
| **Column count** | 96 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `term` | `integer` | `int` | `PK` | `not_null` | (required) TermID |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `cctp` | `string` | `varchar` | `PK` | `not_null` | (required) Coverage Type. Max length: 3 |
| `cotp` | `integer` | `int` | `PK` | `not_null` | (required) Term Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45 |
| `cotp_kw` | `string` | `varchar` |  |  | Term Type (keyword). Allowed values: tsctm.tmtp.material, tsctm.tmtp.labor, tsctm.tmtp.tool, tsctm.tmtp.travel, tsctm.tmtp.subcon, tsctm.tmtp.helpdesk, tsctm.tmtp.other, tsctm.tmtp.uptime, tsctm.tmtp.all |
| `nrbt` | `integer` | `int` | `PK` | `not_null` | (required) Coverage Line |
| `cvtp` | `integer` | `int` |  |  | Coverage Line Type. Allowed values: 1, 2 |
| `cvtp_kw` | `string` | `varchar` |  |  | Coverage Line Type (keyword). Allowed values: tsctm.cvtp.coverage, tsctm.cvtp.claim |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `nrpe` | `integer` | `int` |  |  | Number of Periods |
| `peru` | `integer` | `int` |  |  | Period Unit. Allowed values: 5, 10, 15, 20, 25 |
| `peru_kw` | `string` | `varchar` |  |  | Period Unit (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `stdt` | `date` | `date` |  |  | Start Date |
| `endt` | `date` | `date` |  |  | End Date |
| `cmdl` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sigr` | `string` | `varchar` |  |  | Serialized Item Group. Max length: 6 |
| `limi` | `float` | `float` |  |  | Limiting Counter Value |
| `ccov` | `integer` | `int` |  |  | Cost Covering Method. Allowed values: 5, 10, 15, 20, 22, 23 |
| `ccov_kw` | `string` | `varchar` |  |  | Cost Covering Method (keyword). Allowed values: tsctm.ccov.fixed, tsctm.ccov.ceilpric, tsctm.ccov.disc, tsctm.ccov.ceildisc, tsctm.ccov.exclusion, tsctm.ccov.own.risk |
| `ceil` | `float` | `float` |  |  | Ceiling |
| `disc` | `float` | `float` |  |  | Coverage Percentage |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `amnt` | `float` | `float` |  |  | Sales Amount |
| `ealc_1` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_2` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_3` | `float` | `float` |  |  | Estimated Allocated Costs |
| `eals` | `float` | `float` |  |  | Estimated Allocated Sales |
| `spco_1` | `float` | `float` |  |  | Spent Costs |
| `spco_2` | `float` | `float` |  |  | Spent Costs |
| `spco_3` | `float` | `float` |  |  | Spent Costs |
| `spsa` | `float` | `float` |  |  | Spent Sales |
| `spdi` | `float` | `float` |  |  | Spent Discount |
| `alco_1` | `float` | `float` |  |  | Allocated Costs |
| `alco_2` | `float` | `float` |  |  | Allocated Costs |
| `alco_3` | `float` | `float` |  |  | Allocated Costs |
| `alsa` | `float` | `float` |  |  | Allocated Sales |
| `aldi` | `float` | `float` |  |  | Allocated Discount |
| `clco_1` | `float` | `float` |  |  | Claim Cost Amount |
| `clco_2` | `float` | `float` |  |  | Claim Cost Amount |
| `clco_3` | `float` | `float` |  |  | Claim Cost Amount |
| `clsa` | `float` | `float` |  |  | Claim Revenue Amount |
| `tcam` | `float` | `float` |  |  | Total Ceiling Amount |
| `ccal` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Text |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `cmdl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `sigr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg010 Serialized Item Groups |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camt_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `amnt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `amnt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `spco_cntc` | `float` | `float` |  |  | CC: Spent Cost Amount in Contract Currency |
| `spco_refc` | `float` | `float` |  |  | CC: Spent Cost Amount in Reference Currency |
| `spco_dwhc` | `float` | `float` |  |  | CC: Spent Cost Amount in Data Warehouse Currency |
| `spsa_homc` | `float` | `float` |  |  | CC: Spent Sales Amount in Local Currency |
| `spsa_rpc1` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 1 |
| `spsa_rpc2` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 2 |
| `spsa_refc` | `float` | `float` |  |  | CC: Spent Sales Amount in Reference Currency |
| `spsa_dwhc` | `float` | `float` |  |  | CC: Spent Sales Amount in Data Warehouse Currency |
| `ealc_cntc` | `float` | `float` |  |  | CC: Estimated Allocated Cost in Contract Currency |
| `ealc_refc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Reference Currency |
| `ealc_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Data Warehouse Currency |
| `eals_homc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Local Currency |
| `eals_rpc1` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 1 |
| `eals_rpc2` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 2 |
| `eals_refc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reference Currency |
| `eals_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Data Warehouse Currency |
| `alco_cntc` | `float` | `float` |  |  | CC: Allocated Costs in Contract Currency |
| `alco_refc` | `float` | `float` |  |  | CC: Allocated Costs in Reference Currency |
| `alco_dwhc` | `float` | `float` |  |  | CC: Allocated Costs in Data Warehouse Currency |
| `alsa_homc` | `float` | `float` |  |  | CC: Allocated Sales in Local Currency |
| `alsa_rpc1` | `float` | `float` |  |  | CC: Allocated Sales in Reporting Currency 1 |
| `alsa_rpc2` | `float` | `float` |  |  | CC: Allocated Sales in Reporting Currency 2 |
| `alsa_refc` | `float` | `float` |  |  | CC: Allocated Sales in Reference Currency |
| `alsa_dwhc` | `float` | `float` |  |  | CC: Allocated Sales in Data Warehouse Currency |
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
