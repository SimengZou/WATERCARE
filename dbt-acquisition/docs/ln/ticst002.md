# ticst002

LN ticst002 Estimated and Actual Hours table, Generated 2026-04-10T19:41:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ticst002` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pdno`, `opno` |
| **Column count** | 154 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pdno` | `string` | `varchar` | `PK` | `not_null` | (required) Production Order. Max length: 9 |
| `opno` | `integer` | `int` | `PK` | `not_null` | (required) Operation |
| `tano` | `string` | `varchar` |  |  | Reference Operation. Max length: 8 |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `mcno` | `string` | `varchar` |  |  | Machine Type. Max length: 9 |
| `oprc` | `string` | `varchar` |  |  | Operation Rate Code. Max length: 6 |
| `prte` | `float` | `float` |  |  | Production Rate |
| `rutm` | `float` | `float` |  |  | Cycle Time |
| `sutm` | `float` | `float` |  |  | Setup Time |
| `subr` | `float` | `float` |  |  | Subcontracting Rate Factor |
| `most` | `float` | `float` |  |  | Man Occupation for Setup |
| `mopr` | `float` | `float` |  |  | Man Occupation for Production |
| `mcoc` | `float` | `float` |  |  | Machine Occupation |
| `nomc` | `integer` | `int` |  |  | Number of Machines |
| `bpid` | `string` | `varchar` |  |  | Subcontractor. Max length: 9 |
| `qpln` | `float` | `float` |  |  | Quantity Planned |
| `qcmp` | `float` | `float` |  |  | Quantity Completed |
| `qrjc` | `float` | `float` |  |  | Quantity Scrapped |
| `qqar` | `float` | `float` |  |  | Quantity Quarantined |
| `cums` | `float` | `float` |  |  | Cumulative Scrap |
| `scpq` | `float` | `float` |  |  | Scrap quantity |
| `yldp` | `float` | `float` |  |  | Yield Percentage |
| `hrem` | `float` | `float` |  |  | Estimated Labor Hours (Production) |
| `hrmc` | `float` | `float` |  |  | Estimated Machine Hours (Production) |
| `eshm` | `float` | `float` |  |  | Estimated Labor Hours (Setup) |
| `eshc` | `float` | `float` |  |  | Estimated Machine Hours (Setup) |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `wgcs_1` | `float` | `float` |  |  | Estimated Labor Costs |
| `wgcs_2` | `float` | `float` |  |  | Estimated Labor Costs |
| `wgcs_3` | `float` | `float` |  |  | Estimated Labor Costs |
| `mccs_1` | `float` | `float` |  |  | Estimated Machine Costs |
| `mccs_2` | `float` | `float` |  |  | Estimated Machine Costs |
| `mccs_3` | `float` | `float` |  |  | Estimated Machine Costs |
| `ohcs_1` | `float` | `float` |  |  | Estimated Overhead Costs |
| `ohcs_2` | `float` | `float` |  |  | Estimated Overhead Costs |
| `ohcs_3` | `float` | `float` |  |  | Estimated Overhead Costs |
| `sccs_1` | `float` | `float` |  |  | Estimated Subcontracting Costs |
| `sccs_2` | `float` | `float` |  |  | Estimated Subcontracting Costs |
| `sccs_3` | `float` | `float` |  |  | Estimated Subcontracting Costs |
| `ahma` | `float` | `float` |  |  | Actual Labor Hours (Production) |
| `ahws` | `float` | `float` |  |  | Delta Actual Split Labor Hours (Production) |
| `ahwq` | `float` | `float` |  |  | Delta Actual Labor Hours to Quarantine(Production) |
| `ahmc` | `float` | `float` |  |  | Actual Machine Hours (Production) |
| `ahms` | `float` | `float` |  |  | Delta Actual Split Machine Hours (Production) |
| `ahmq` | `float` | `float` |  |  | Delta Actual Machine Hours to Quarantine(Production) |
| `ashm` | `float` | `float` |  |  | Actual Labor Hours (Setup) |
| `ashs` | `float` | `float` |  |  | Delta Actual Split Labor Hours (Setup) |
| `ashq` | `float` | `float` |  |  | Delta Actual Labor Hours to Quarantine(Setup) |
| `ashc` | `float` | `float` |  |  | Actual Machine Hours (Setup) |
| `asms` | `float` | `float` |  |  | Delta Actual Split Machine Hours (Setup) |
| `asmq` | `float` | `float` |  |  | Delta Actual Machine Hours to Quarantine(Setup) |
| `amcc_1` | `float` | `float` |  |  | Actual Machine Costs (Aggregated) |
| `amcc_2` | `float` | `float` |  |  | Actual Machine Costs (Aggregated) |
| `amcc_3` | `float` | `float` |  |  | Actual Machine Costs (Aggregated) |
| `aohc_1` | `float` | `float` |  |  | Actual Overhead Costs (Aggregated) |
| `aohc_2` | `float` | `float` |  |  | Actual Overhead Costs (Aggregated) |
| `aohc_3` | `float` | `float` |  |  | Actual Overhead Costs (Aggregated) |
| `awgc_1` | `float` | `float` |  |  | Actual Labor Costs (Aggregated) |
| `awgc_2` | `float` | `float` |  |  | Actual Labor Costs (Aggregated) |
| `awgc_3` | `float` | `float` |  |  | Actual Labor Costs (Aggregated) |
| `ascc_1` | `float` | `float` |  |  | Actual Subcontracting Costs (Aggregated) |
| `ascc_2` | `float` | `float` |  |  | Actual Subcontracting Costs (Aggregated) |
| `ascc_3` | `float` | `float` |  |  | Actual Subcontracting Costs (Aggregated) |
| `ascq_1` | `float` | `float` |  |  | Delta Actual Subcontracting Costs to Quarantine |
| `ascq_2` | `float` | `float` |  |  | Delta Actual Subcontracting Costs to Quarantine |
| `ascq_3` | `float` | `float` |  |  | Delta Actual Subcontracting Costs to Quarantine |
| `runi` | `float` | `float` |  |  | Routing Unit |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `pcdt` | `timestamp` | `timestamp_ntz` |  |  | Partial Completion Date |
| `fdur` | `integer` | `int` |  |  | Fixed Duration. Allowed values: 1, 2 |
| `fdur_kw` | `string` | `varchar` |  |  | Fixed Duration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cowc` | `string` | `varchar` |  |  | Costing Work Center. Max length: 6 |
| `frso` | `integer` | `int` |  |  | Frozen Operation. Allowed values: 1, 2 |
| `frso_kw` | `string` | `varchar` |  |  | Frozen Operation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pdno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc001 Production Orders |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `oprc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ticpr050 Operation Rate Codes |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `wgcs_lclc` | `float` | `float` |  |  | CC: Estimate Labor Costs in Local Currency |
| `wgcs_rpc1` | `float` | `float` |  |  | CC: Estimate Labor Costs in Reporting Currency 1 |
| `wgcs_rpc2` | `float` | `float` |  |  | CC: Estimate Labor Costs in Reporting Currency 2 |
| `wgcs_refc` | `float` | `float` |  |  | CC: Estimate Labor Costs in Reference Currency |
| `wgcs_dwhc` | `float` | `float` |  |  | CC: Estimate Labor Costs in Data Warehouse Currency |
| `mccs_lclc` | `float` | `float` |  |  | CC: Estimate Machine Costs in Local Currency |
| `mccs_rpc1` | `float` | `float` |  |  | CC: Estimate Machine Costs in Reporting Currency 1 |
| `mccs_rpc2` | `float` | `float` |  |  | CC: Estimate Machine Costs in Reporting Currency 2 |
| `mccs_refc` | `float` | `float` |  |  | CC: Estimate Machine Costs in Reference Currency |
| `mccs_dwhc` | `float` | `float` |  |  | CC: Estimate Machine Costs in Data Warehouse Currency |
| `ohcs_lclc` | `float` | `float` |  |  | CC: Estimate Overhead Costs in Local Currency |
| `ohcs_rpc1` | `float` | `float` |  |  | CC: Estimate Overhead Costs in Reporting Currency 1 |
| `ohcs_rpc2` | `float` | `float` |  |  | CC: Estimate Overhead Costs in Reporting Currency 2 |
| `ohcs_refc` | `float` | `float` |  |  | CC: Estimate Overhead Costs in Reference Currency |
| `ohcs_dwhc` | `float` | `float` |  |  | CC: Estimate Overhead Costs in Data Warehouse Currency |
| `sccs_lclc` | `float` | `float` |  |  | CC: Estimate Subcontracting Costs in Local Currency |
| `sccs_rpc1` | `float` | `float` |  |  | CC: Estimate Subcontracting Costs in Reporting Currency 1 |
| `sccs_rpc2` | `float` | `float` |  |  | CC: Estimate Subcontracting Costs in Reporting Currency 2 |
| `sccs_refc` | `float` | `float` |  |  | CC: Estimate Subcontracting Costs in Reference Currency |
| `sccs_dwhc` | `float` | `float` |  |  | CC: Estimate Subcontracting Costs in Data Warehouse Currency |
| `amcc_lclc` | `float` | `float` |  |  | CC: Actual Machine Costs (Aggregated) in Local Currency |
| `amcc_rpc1` | `float` | `float` |  |  | CC: Actual Machine Costs (Aggregated) in Reporting Currency 1 |
| `amcc_rpc2` | `float` | `float` |  |  | CC: Actual Machine Costs (Aggregated) in Reporting Currency 2 |
| `amcc_refc` | `float` | `float` |  |  | CC: Actual Machine Costs (Aggregated) in Reference Currency |
| `amcc_dwhc` | `float` | `float` |  |  | CC: Actual Machine Costs (Aggregated) in Data Warehouse Currency |
| `aohc_lclc` | `float` | `float` |  |  | CC: Actual Overhead Costs (Aggregated) in Local Currency |
| `aohc_rpc1` | `float` | `float` |  |  | CC: Actual Overhead Costs (Aggregated) in Reporting Currency 1 |
| `aohc_rpc2` | `float` | `float` |  |  | CC: Actual Overhead Costs (Aggregated) in Reporting Currency 2 |
| `aohc_refc` | `float` | `float` |  |  | CC: Actual Overhead Costs (Aggregated) in Reference Currency |
| `aohc_dwhc` | `float` | `float` |  |  | CC: Actual Overhead Costs (Aggregated) in Data Warehouse Currency |
| `awgc_lclc` | `float` | `float` |  |  | CC: Actual Labor Costs (Aggregated) in Local Currency |
| `awgc_rpc1` | `float` | `float` |  |  | CC: Actual Labor Costs (Aggregated) in Reporting Currency 1 |
| `awgc_rpc2` | `float` | `float` |  |  | CC: Actual Labor Costs (Aggregated) in Reporting Currency 2 |
| `awgc_refc` | `float` | `float` |  |  | CC: Actual Labor Costs (Aggregated) in Reference Currency |
| `awgc_dwhc` | `float` | `float` |  |  | CC: Actual Labor Costs (Aggregated) in Data Warehouse Currency |
| `ascc_lclc` | `float` | `float` |  |  | CC: Actual Subcontract Costs (Aggregated) in Local Currency |
| `ascc_rpc1` | `float` | `float` |  |  | CC: Actual Subcontract Costs (Aggregated) in Reporting Currency 1 |
| `ascc_rpc2` | `float` | `float` |  |  | CC: Actual Subcontract Costs (Aggregated) in Reporting Currency 2 |
| `ascc_refc` | `float` | `float` |  |  | CC: Actual Subcontract Costs (Aggregated) in Reference Currency |
| `ascc_dwhc` | `float` | `float` |  |  | CC: Actual Subcontract Costs (Aggregated) in Data Warehouse Currency |
| `ascq_lclc` | `float` | `float` |  |  | CC: Delta Actual Subcontract Costs to Quarantine in Local Currency |
| `ascq_rpc1` | `float` | `float` |  |  | CC: Delta Actual Subcontract Costs to Quarantine in Reporting Currency |
| `ascq_rpc2` | `float` | `float` |  |  | CC: Delta Actual Subcontract Costs to Quarantine in Reporting Currency |
| `ascq_refc` | `float` | `float` |  |  | CC: Delta Actual Subcontract Costs to Quarantine in Reference Currency |
| `ascq_dwhc` | `float` | `float` |  |  | CC: Delta Actual Subcontract Costs to Quarantine in Data Warehouse Cur |
| `dptm_fcmp` | `integer` | `int` |  |  | CC: Financial Company of Department |
| `scpq_buar` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Area |
| `scpq_buln` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Length |
| `scpq_bupc` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Piece |
| `scpq_butm` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Time |
| `scpq_buvl` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Volume |
| `scpq_buwg` | `float` | `float` |  |  | CC: Scrap Quantity in Base Unit Weight |
| `qrjc_buar` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Area |
| `qrjc_buln` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Length |
| `qrjc_bupc` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Piece |
| `qrjc_butm` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Time |
| `qrjc_buvl` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Volume |
| `qrjc_buwg` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Weight |
| `qqar_buar` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Area |
| `qqar_buln` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Length |
| `qqar_bupc` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Piece |
| `qqar_butm` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Time |
| `qqar_buvl` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Volume |
| `qqar_buwg` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Weight |
| `mcno_ref_compnr` | `integer` | `int` |  |  | CC: Reference Company of Machine Type |
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
