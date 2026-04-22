# tsctm136

LN tsctm136 Other Terms table, Generated 2026-04-10T19:42:32Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm136` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln`, `cctp`, `cotp`, `nrbt`, `cseq` |
| **Column count** | 108 |

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
| `cseq` | `integer` | `int` | `PK` | `not_null` | (required) Other Line |
| `otbp` | `string` | `varchar` |  |  | Subcontractor. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `ccmp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `pric_1` | `float` | `float` |  |  | Unit Cost |
| `pric_2` | `float` | `float` |  |  | Unit Cost |
| `pric_3` | `float` | `float` |  |  | Unit Cost |
| `pris` | `float` | `float` |  |  | Sales Price |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `amnt` | `float` | `float` |  |  | Sales Amount |
| `dspc` | `integer` | `int` |  |  | Term is Derived from Reference Activity. Allowed values: 1, 2 |
| `dspc_kw` | `string` | `varchar` |  |  | Term is Derived from Reference Activity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spqu` | `float` | `float` |  |  | Spent Quantity |
| `spco_1` | `float` | `float` |  |  | Spent Costs |
| `spco_2` | `float` | `float` |  |  | Spent Costs |
| `spco_3` | `float` | `float` |  |  | Spent Costs |
| `spsa` | `float` | `float` |  |  | Spent Sales |
| `alqu` | `float` | `float` |  |  | Allocated Quantity |
| `alco_1` | `float` | `float` |  |  | Allocated Costs |
| `alco_2` | `float` | `float` |  |  | Allocated Costs |
| `alco_3` | `float` | `float` |  |  | Allocated Costs |
| `alsa` | `float` | `float` |  |  | Allocated Sales |
| `ealc_1` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_2` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_3` | `float` | `float` |  |  | Estimated Allocated Costs |
| `eals` | `float` | `float` |  |  | Estimated Allocated Sales |
| `eaqu` | `float` | `float` |  |  | Estimated Allocated Quantity |
| `txta` | `integer` | `int` |  |  | Text |
| `term_cfln_cctp_cotp_nrbt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm120 Coverage Terms |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `ccmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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
| `alqu_buar` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Area |
| `alqu_buln` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Length |
| `alqu_buvl` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Volume |
| `alqu_buwg` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Weight |
| `alqu_bupc` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Piece |
| `alqu_invu` | `float` | `float` |  |  | CC: Allocated Quantity in Inventory Unit |
| `quan_bupc` | `float` | `float` |  |  | CC: Quantity in Base Unit Piece |
| `quan_invu` | `float` | `float` |  |  | CC: Quantity in Inventory Unit |
| `quan_buar` | `float` | `float` |  |  | CC: Quantity in Base Unit Area |
| `quan_buln` | `float` | `float` |  |  | CC: Quantity in Base Unit Length |
| `quan_buvl` | `float` | `float` |  |  | CC: Quantity in Base Unit Volume |
| `quan_buwg` | `float` | `float` |  |  | CC: Quantity in Base Unit Weight |
| `eaqu_buln` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Length |
| `eaqu_butm` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Time |
| `eaqu_buvl` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Volume |
| `eaqu_buwg` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Weight |
| `eaqu_bupc` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Piece |
| `eaqu_invu` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Inventory Unit |
| `eaqu_buar` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Area |
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
