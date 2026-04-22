# txptc904

LN txptc904 Change Request Lines table, Generated 2026-04-10T19:42:43Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_txptc904` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `creq`, `pono` |
| **Column count** | 119 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `creq` | `string` | `varchar` | `PK` | `not_null` | (required) Change Request. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `rtyp` | `integer` | `int` |  |  | Request Type. Allowed values: 1, 2, 3 |
| `rtyp_kw` | `string` | `varchar` |  |  | Request Type (keyword). Allowed values: txtype.creq, txtype.vreq, txtype.not.app |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `risk` | `string` | `varchar` |  |  | Risk. Max length: 9 |
| `cono` | `string` | `varchar` |  |  | Purchase Contract. Max length: 9 |
| `cnln` | `integer` | `int` |  |  | Contract Line |
| `pdat` | `timestamp` | `timestamp_ntz` |  |  | Processed Date |
| `bcav` | `string` | `varchar` |  |  | Budget cost analysis version. Max length: 3 |
| `acti` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `reqc` | `integer` | `int` |  |  | Required Change |
| `irej` | `integer` | `int` |  |  | Impact of Rejection |
| `pout` | `integer` | `int` |  |  | Project Outcome. Allowed values: 1, 2 |
| `pout_kw` | `string` | `varchar` |  |  | Project Outcome (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cost` | `integer` | `int` |  |  | Cost. Allowed values: 1, 2 |
| `cost_kw` | `string` | `varchar` |  |  | Cost (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sche` | `integer` | `int` |  |  | Schedule. Allowed values: 1, 2 |
| `sche_kw` | `string` | `varchar` |  |  | Schedule (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scpe` | `integer` | `int` |  |  | Scope. Allowed values: 1, 2 |
| `scpe_kw` | `string` | `varchar` |  |  | Scope (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cont` | `integer` | `int` |  |  | Contract. Allowed values: 1, 2 |
| `cont_kw` | `string` | `varchar` |  |  | Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `camp` | `integer` | `int` |  |  | AMP. Allowed values: 0, 1, 2 |
| `camp_kw` | `string` | `varchar` |  |  | AMP (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `ipou` | `integer` | `int` |  |  | Impact on Project outcome |
| `icos` | `integer` | `int` |  |  | Impact on Cost |
| `isch` | `integer` | `int` |  |  | Impact on Schedule |
| `iscp` | `integer` | `int` |  |  | Impact on Scope |
| `icon` | `integer` | `int` |  |  | Impact on Contract |
| `iamp` | `integer` | `int` |  |  | Imapct on AMP |
| `vamt` | `float` | `float` |  |  | Variation Request Amount |
| `camt` | `float` | `float` |  |  | Change Request Amount |
| `cacc_1` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_2` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_3` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_4` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_5` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_6` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_7` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_8` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_9` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `cacc_10` | `string` | `varchar` |  |  | Control Account. Max length: 50 |
| `ssdt_1` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_2` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_3` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_4` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_5` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_6` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_7` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_8` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_9` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `ssdt_10` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `sfdt_1` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_2` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_3` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_4` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_5` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_6` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_7` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_8` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_9` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `sfdt_10` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `rsdt_1` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_2` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_3` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_4` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_5` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_6` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_7` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_8` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_9` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rsdt_10` | `timestamp` | `timestamp_ntz` |  |  | Revised Start Date |
| `rfdt_1` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_2` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_3` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_4` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_5` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_6` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_7` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_8` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_9` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `rfdt_10` | `timestamp` | `timestamp_ntz` |  |  | Revised Finish Date |
| `actp` | `integer` | `int` |  |  | Activity Details Present. Allowed values: 0, 1, 2 |
| `actp_kw` | `string` | `varchar` |  |  | Activity Details Present (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `sact_1` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_2` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_3` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_4` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_5` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_6` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_7` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_8` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_9` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `sact_10` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `piba` | `integer` | `int` |  |  | Is the project included in Budget or AMP. Allowed values: 0, 1, 2 |
| `piba_kw` | `string` | `varchar` |  |  | Is the project included in Budget or AMP (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `creq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table txptc903 Change Request Header |
| `risk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table txptc902 Project Risks |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `reqc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `irej_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `ipou_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `icos_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `isch_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `iscp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `icon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `iamp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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
