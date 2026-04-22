# tsctm135

LN tsctm135 Helpdesk Terms table, Generated 2026-04-10T19:42:32Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm135` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln`, `cctp`, `cotp`, `nrbt`, `cseq` |
| **Column count** | 106 |

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
| `cseq` | `integer` | `int` | `PK` | `not_null` | (required) Helpdesk Line |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `nrcl` | `integer` | `int` |  |  | Number of Help Desk Calls |
| `nrun` | `integer` | `int` |  |  | Number of Call Units |
| `clrt` | `string` | `varchar` |  |  | Labor Rate Code. Max length: 6 |
| `cuni` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `cinv` | `string` | `varchar` |  |  | Invoice Interval. Max length: 3 |
| `ccmp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `wrac_1` | `float` | `float` |  |  | Cost Rate |
| `wrac_2` | `float` | `float` |  |  | Cost Rate |
| `wrac_3` | `float` | `float` |  |  | Cost Rate |
| `wras` | `float` | `float` |  |  | Sales Rate |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `amnt` | `float` | `float` |  |  | Sales Amount |
| `crtc` | `string` | `varchar` |  |  | Response Time. Max length: 3 |
| `rstu` | `string` | `varchar` |  |  | Response Time Unit. Max length: 3 |
| `rctt` | `float` | `float` |  |  | Reaction Period |
| `slst` | `float` | `float` |  |  | Solution Start Period |
| `slfn` | `float` | `float` |  |  | Solution Finish Period |
| `ccal` | `string` | `varchar` |  |  | Coverage Time Calendar for Helpdesk. Max length: 9 |
| `rsin` | `integer` | `int` |  |  | Response Time Definition Method. Allowed values: 1, 2 |
| `rsin_kw` | `string` | `varchar` |  |  | Response Time Definition Method (keyword). Allowed values: tsclm.rein.resp, tsclm.rein.busd |
| `bsls` | `integer` | `int` |  |  | Business Day Counter Solution Start |
| `bsfn` | `integer` | `int` |  |  | Business Day Counter Solution Finish |
| `srcl` | `string` | `varchar` |  |  | Coverage Time Calendar for Solution Limits. Max length: 9 |
| `spcl` | `integer` | `int` |  |  | Spent Calls |
| `spun` | `integer` | `int` |  |  | Spent Call Units |
| `spco_1` | `float` | `float` |  |  | Spent Costs |
| `spco_2` | `float` | `float` |  |  | Spent Costs |
| `spco_3` | `float` | `float` |  |  | Spent Costs |
| `spsa` | `float` | `float` |  |  | Spent Sales |
| `alcl` | `integer` | `int` |  |  | Allocated Calls |
| `alun` | `integer` | `int` |  |  | Allocated Call Units |
| `alco_1` | `float` | `float` |  |  | Allocated Costs |
| `alco_2` | `float` | `float` |  |  | Allocated Costs |
| `alco_3` | `float` | `float` |  |  | Allocated Costs |
| `alsa` | `float` | `float` |  |  | Allocated Sales |
| `ealc_1` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_2` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_3` | `float` | `float` |  |  | Estimated Allocated Costs |
| `eals` | `float` | `float` |  |  | Estimated Allocated Sales |
| `eacl` | `integer` | `int` |  |  | Estimated Allocated Calls |
| `eaun` | `integer` | `int` |  |  | Estimated Allocated Call Units |
| `txta` | `integer` | `int` |  |  | Text |
| `term_cfln_cctp_cotp_nrbt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm120 Coverage Terms |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cinv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm070 Time Intervals for Invoicing |
| `ccmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `crtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm020 Response Types |
| `rstu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `srcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
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
| `alco_cntc` | `float` | `float` |  |  | CC: Allocated Cost in Contract Currency |
| `alco_refc` | `float` | `float` |  |  | CC: Allocated Cost Amount in Reference Currency |
| `alco_dwhc` | `float` | `float` |  |  | CC: Allocated Cost Amount in Data Warehouse Currency |
| `alsa_homc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Local Currency |
| `alsa_rpc1` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reporting Currency 1 |
| `alsa_rpc2` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reporting Currency 2 |
| `alsa_refc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reference Currency |
| `alsa_dwhc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Data Warehouse Currency |
| `ealc_cntc` | `float` | `float` |  |  | CC: Estimated Allocated Cost in Contract Currency |
| `ealc_refc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Reference Currency |
| `ealc_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Data Warehouse Currency |
| `eals_homc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Local Currency |
| `eals_rpc1` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 1 |
| `eals_rpc2` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 2 |
| `eals_refc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reference Currency |
| `eals_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Data Warehouse Currency |
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
