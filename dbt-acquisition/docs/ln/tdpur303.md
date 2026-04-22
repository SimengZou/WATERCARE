# tdpur303

LN tdpur303 Purchase Contract Prices table, Generated 2026-04-10T19:41:21Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur303` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `pono`, `cofc`, `csqn`, `sdat` |
| **Column count** | 112 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Office. Max length: 6 |
| `csqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `sdat` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Effective Date |
| `revn` | `integer` | `int` |  |  | Contract Price Line |
| `icap` | `integer` | `int` |  |  | Contract Prices status. Allowed values: 1, 2, 3 |
| `icap_kw` | `string` | `varchar` |  |  | Contract Prices status (keyword). Allowed values: tcicap.free, tcicap.active, tcicap.closed |
| `pric` | `float` | `float` |  |  | Price |
| `cupp` | `string` | `varchar` |  |  | Purchase Price Unit. Max length: 3 |
| `cvpp` | `float` | `float` |  |  | Conversion Factor Price to Inventory Unit |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `prbk` | `string` | `varchar` |  |  | Price Book. Max length: 9 |
| `rsel` | `integer` | `int` |  |  | Selected for Retro-billing. Allowed values: 1, 2 |
| `rsel_kw` | `string` | `varchar` |  |  | Selected for Retro-billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `disc_1` | `float` | `float` |  |  | Discount % |
| `disc_2` | `float` | `float` |  |  | Discount % |
| `disc_3` | `float` | `float` |  |  | Discount % |
| `disc_4` | `float` | `float` |  |  | Discount % |
| `disc_5` | `float` | `float` |  |  | Discount % |
| `disc_6` | `float` | `float` |  |  | Discount % |
| `disc_7` | `float` | `float` |  |  | Discount % |
| `disc_8` | `float` | `float` |  |  | Discount % |
| `disc_9` | `float` | `float` |  |  | Discount % |
| `disc_10` | `float` | `float` |  |  | Discount % |
| `disc_11` | `float` | `float` |  |  | Discount % |
| `ldam_1` | `float` | `float` |  |  | Discount Amount |
| `ldam_2` | `float` | `float` |  |  | Discount Amount |
| `ldam_3` | `float` | `float` |  |  | Discount Amount |
| `ldam_4` | `float` | `float` |  |  | Discount Amount |
| `ldam_5` | `float` | `float` |  |  | Discount Amount |
| `ldam_6` | `float` | `float` |  |  | Discount Amount |
| `ldam_7` | `float` | `float` |  |  | Discount Amount |
| `ldam_8` | `float` | `float` |  |  | Discount Amount |
| `ldam_9` | `float` | `float` |  |  | Discount Amount |
| `ldam_10` | `float` | `float` |  |  | Discount Amount |
| `ldam_11` | `float` | `float` |  |  | Discount Amount |
| `dmth_1` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_1` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_2` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_2` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_3` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_3` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_4` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_4` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_5` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_5` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_6` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_6` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_7` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_7` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_8` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_8` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_9` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_9` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_10` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_10` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_11` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_11` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `cdis_1` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_2` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_3` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_4` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_5` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_6` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_7` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_8` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_9` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_10` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_11` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `dssc_1` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_2` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_3` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_4` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_5` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_6` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_7` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_8` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_9` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_10` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_11` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dtrm` | `integer` | `int` |  |  | Determining. Allowed values: 1, 2 |
| `dtrm_kw` | `string` | `varchar` |  |  | Determining (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elgb` | `integer` | `int` |  |  | Eligible. Allowed values: 1, 2 |
| `elgb_kw` | `string` | `varchar` |  |  | Eligible (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpwc` | `integer` | `int` |  |  | Cumulative Price Break. Allowed values: 1, 2 |
| `cpwc_kw` | `string` | `varchar` |  |  | Cumulative Price Break (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tpbk` | `string` | `varchar` |  |  | Target Price Book. Max length: 9 |
| `tpbl` | `integer` | `int` |  |  | Target Price Book Line |
| `tapr` | `float` | `float` |  |  | Target Price |
| `pbor` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4 |
| `pbor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpcg.pbor.stnd, tdpcg.pbor.socn, tdpcg.pbor.pocn, tdpcg.pbor.rfqo |
| `dsor` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4, 5 |
| `dsor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpcg.dsor.stnd, tdpcg.dsor.socn, tdpcg.dsor.pocn, tdpcg.dsor.rfqo, tdpcg.dsor.prom |
| `txtr` | `integer` | `int` |  |  | Revision Text |
| `cono_pono_cofc_csqn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur301 Purchase Contract Lines |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `prbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg011 Price Books |
| `tpbk_tpbl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg033 Target Price Book Lines |
| `tpbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg013 Target Price Books |
| `txtr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
