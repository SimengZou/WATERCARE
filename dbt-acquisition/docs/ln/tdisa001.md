# tdisa001

LN tdisa001 Item - Sales table, Generated 2026-04-10T19:41:17Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdisa001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item` |
| **Column count** | 83 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key I - base language. Max length: 16 |
| `qimc` | `float` | `float` |  |  | Minimum Quantity to Commit |
| `mnra` | `float` | `float` |  |  | Minimum Rate for Commitment |
| `qimo` | `float` | `float` |  |  | Minimum Order Quantity |
| `qpmo` | `float` | `float` |  |  | Minimum Order Quantity in Piece Unit |
| `cuqs` | `string` | `varchar` |  |  | Sales Unit. Max length: 3 |
| `cuqi` | `string` | `varchar` |  |  | Sales Piece Unit. Max length: 3 |
| `cups` | `string` | `varchar` |  |  | Sales Price Unit. Max length: 3 |
| `cpgs` | `string` | `varchar` |  |  | Sales Price Group. Max length: 6 |
| `csgs` | `string` | `varchar` |  |  | Sales Statistics Group. Max length: 6 |
| `cmgp` | `string` | `varchar` |  |  | Commission Group. Max length: 6 |
| `rbgp` | `string` | `varchar` |  |  | Sales Rebate group. Max length: 6 |
| `ccur` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `ltsp` | `timestamp` | `timestamp_ntz` |  |  | Last Sales Price Transaction Date |
| `prir` | `float` | `float` |  |  | Suggested Retail Price |
| `umsp` | `float` | `float` |  |  | Upper Margin |
| `lmsp` | `float` | `float` |  |  | Lower Margin |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `qidd` | `float` | `float` |  |  | Direct Delivery from Quantity |
| `qpdd` | `float` | `float` |  |  | Direct Delivery from Quantity in Piece Unit |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `acci` | `integer` | `int` |  |  | Accessories Allowed. Allowed values: 1, 2 |
| `acci_kw` | `string` | `varchar` |  |  | Accessories Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `retw` | `integer` | `int` |  |  | Release to Warehousing. Allowed values: 1, 2 |
| `retw_kw` | `string` | `varchar` |  |  | Release to Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acom` | `integer` | `int` |  |  | Commitment Policy. Allowed values: 1, 2, 3, 4 |
| `acom_kw` | `string` | `varchar` |  |  | Commitment Policy (keyword). Allowed values: tdacom.no, tdacom.entry, tdacom.batch, tdacom.manual |
| `rcom` | `integer` | `int` |  |  | Return Components. Allowed values: 1, 2 |
| `rcom_kw` | `string` | `varchar` |  |  | Return Components (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `afrb` | `integer` | `int` |  |  | Item applicable for Retro-Billing. Allowed values: 1, 2 |
| `afrb_kw` | `string` | `varchar` |  |  | Item applicable for Retro-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcmp` | `integer` | `int` |  |  | Site Logistic Company |
| `sfsi` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cphl` | `integer` | `int` |  |  | Component Handling. Allowed values: 5, 10, 15 |
| `cphl_kw` | `string` | `varchar` |  |  | Component Handling (keyword). Allowed values: tdcphl.not.applicable, tdcphl.sales.bom, tdcphl.component.lines |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tdscon.none, tdscon.sc, tdscon.rc, tdscon.oc, tdscon.sca, tdscon.kc, tdscon.not.applicable |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `alod` | `integer` | `int` |  |  | Allow Overdeliveries. Allowed values: 1, 2 |
| `alod_kw` | `string` | `varchar` |  |  | Allow Overdeliveries (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tltp` | `integer` | `int` |  |  | Tolerance Type. Allowed values: 1, 2, 3 |
| `tltp_kw` | `string` | `varchar` |  |  | Tolerance Type (keyword). Allowed values: tcisa.tltp.percentage, tcisa.tltp.quantity, tcisa.tltp.no |
| `actn` | `integer` | `int` |  |  | Action. Allowed values: 1, 2, 3 |
| `actn_kw` | `string` | `varchar` |  |  | Action (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `mmtl` | `float` | `float` |  |  | Maximum Tolerance |
| `mmtp` | `float` | `float` |  |  | Maximum Tolerance in Piece Unit |
| `txts` | `integer` | `int` |  |  | Item Sales Text |
| `item_sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cuqs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuqi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cups_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cpgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `csgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `cmgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdcms018 Commission/Rebate Group |
| `rbgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdcms018 Commission/Rebate Group |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `txts_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `pris_lclc` | `float` | `float` |  |  | CC: Item Sales Price in Local Currency |
| `pris_rpc1` | `float` | `float` |  |  | CC: Item Sales Price in Reporting Currency 1 |
| `pris_rpc2` | `float` | `float` |  |  | CC: Item Sales Price in Reporting Currency 2 |
| `pris_rfrc` | `float` | `float` |  |  | CC: Item Sales Price in Reference Currency |
| `pris_dtwc` | `float` | `float` |  |  | CC: Item Sales Price in Data Warehouse Currency |
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
