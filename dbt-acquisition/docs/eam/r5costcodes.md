# r5costcodes

eam_R5COSTCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5costcodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `cst_code` |
| **Column count** | 67 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cst_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CST_LASTSAVED |
| `cst_udfchkbox05` | `string` | `varchar` |  |  | CST_UDFCHKBOX05 |
| `cst_class` | `string` | `varchar` |  |  | CST_CLASS |
| `cst_notused` | `string` | `varchar` |  |  | CST_NOTUSED |
| `cst_org` | `string` | `varchar` |  |  | CST_ORG |
| `cst_class_org` | `string` | `varchar` |  |  | CST_CLASS_ORG |
| `cst_created` | `timestamp` | `timestamp_ntz` |  |  | CST_CREATED |
| `cst_createdby` | `string` | `varchar` |  |  | CST_CREATEDBY |
| `cst_updated` | `timestamp` | `timestamp_ntz` |  |  | CST_UPDATED |
| `cst_updatedby` | `string` | `varchar` |  |  | CST_UPDATEDBY |
| `cst_updatecount` | `float` | `float` |  |  | CST_UPDATECOUNT |
| `cst_fleetcustomer` | `string` | `varchar` |  |  | CST_FLEETCUSTOMER |
| `cst_fleetcustomer_org` | `string` | `varchar` |  |  | CST_FLEETCUSTOMER_ORG |
| `cst_nonbillable` | `string` | `varchar` |  |  | CST_NONBILLABLE |
| `cst_segmentvalue` | `string` | `varchar` |  |  | CST_SEGMENTVALUE |
| `cst_udfchar01` | `string` | `varchar` |  |  | CST_UDFCHAR01 |
| `cst_udfchar02` | `string` | `varchar` |  |  | CST_UDFCHAR02 |
| `cst_udfchar03` | `string` | `varchar` |  |  | CST_UDFCHAR03 |
| `cst_udfchar04` | `string` | `varchar` |  |  | CST_UDFCHAR04 |
| `cst_udfchar05` | `string` | `varchar` |  |  | CST_UDFCHAR05 |
| `cst_udfchar06` | `string` | `varchar` |  |  | CST_UDFCHAR06 |
| `cst_udfchar07` | `string` | `varchar` |  |  | CST_UDFCHAR07 |
| `cst_udfchar08` | `string` | `varchar` |  |  | CST_UDFCHAR08 |
| `cst_udfchar09` | `string` | `varchar` |  |  | CST_UDFCHAR09 |
| `cst_udfchar10` | `string` | `varchar` |  |  | CST_UDFCHAR10 |
| `cst_udfchar12` | `string` | `varchar` |  |  | CST_UDFCHAR12 |
| `cst_udfchar13` | `string` | `varchar` |  |  | CST_UDFCHAR13 |
| `cst_udfchar14` | `string` | `varchar` |  |  | CST_UDFCHAR14 |
| `cst_udfchar15` | `string` | `varchar` |  |  | CST_UDFCHAR15 |
| `cst_udfchar16` | `string` | `varchar` |  |  | CST_UDFCHAR16 |
| `cst_udfchar17` | `string` | `varchar` |  |  | CST_UDFCHAR17 |
| `cst_udfchar18` | `string` | `varchar` |  |  | CST_UDFCHAR18 |
| `cst_udfchar19` | `string` | `varchar` |  |  | CST_UDFCHAR19 |
| `cst_udfchar20` | `string` | `varchar` |  |  | CST_UDFCHAR20 |
| `cst_udfchar21` | `string` | `varchar` |  |  | CST_UDFCHAR21 |
| `cst_udfchar22` | `string` | `varchar` |  |  | CST_UDFCHAR22 |
| `cst_udfchar23` | `string` | `varchar` |  |  | CST_UDFCHAR23 |
| `cst_udfchar24` | `string` | `varchar` |  |  | CST_UDFCHAR24 |
| `cst_udfchar25` | `string` | `varchar` |  |  | CST_UDFCHAR25 |
| `cst_udfchar26` | `string` | `varchar` |  |  | CST_UDFCHAR26 |
| `cst_udfchar27` | `string` | `varchar` |  |  | CST_UDFCHAR27 |
| `cst_udfchar28` | `string` | `varchar` |  |  | CST_UDFCHAR28 |
| `cst_udfchar29` | `string` | `varchar` |  |  | CST_UDFCHAR29 |
| `cst_udfchar30` | `string` | `varchar` |  |  | CST_UDFCHAR30 |
| `cst_udfnum01` | `float` | `float` |  |  | CST_UDFNUM01 |
| `cst_udfchar11` | `string` | `varchar` |  |  | CST_UDFCHAR11 |
| `cst_udfnum02` | `float` | `float` |  |  | CST_UDFNUM02 |
| `cst_udfnum03` | `float` | `float` |  |  | CST_UDFNUM03 |
| `cst_udfnum04` | `float` | `float` |  |  | CST_UDFNUM04 |
| `cst_udfnum05` | `float` | `float` |  |  | CST_UDFNUM05 |
| `cst_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | CST_UDFDATE01 |
| `cst_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | CST_UDFDATE02 |
| `cst_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | CST_UDFDATE03 |
| `cst_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | CST_UDFDATE04 |
| `cst_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | CST_UDFDATE05 |
| `cst_udfchkbox01` | `string` | `varchar` |  |  | CST_UDFCHKBOX01 |
| `cst_udfchkbox02` | `string` | `varchar` |  |  | CST_UDFCHKBOX02 |
| `cst_udfchkbox03` | `string` | `varchar` |  |  | CST_UDFCHKBOX03 |
| `cst_udfchkbox04` | `string` | `varchar` |  |  | CST_UDFCHKBOX04 |
| `cst_code` | `string` | `varchar` | `PK` | `unique` | CST_CODE |
| `cst_desc` | `string` | `varchar` |  |  | CST_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
