# r5dockreceipts

eam_R5DOCKRECEIPTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dockreceipts` |
| **Materialization** | `incremental` |
| **Primary keys** | `dck_code` |
| **Column count** | 78 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dck_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DCK_LASTSAVED |
| `dck_code` | `string` | `varchar` | `PK` | `unique` | DCK_CODE |
| `dck_org` | `string` | `varchar` |  |  | DCK_ORG |
| `dck_desc` | `string` | `varchar` |  |  | DCK_DESC |
| `dck_status` | `string` | `varchar` |  |  | DCK_STATUS |
| `dck_rstatus` | `string` | `varchar` |  |  | DCK_RSTATUS |
| `dck_supplier` | `string` | `varchar` |  |  | DCK_SUPPLIER |
| `dck_supplier_org` | `string` | `varchar` |  |  | DCK_SUPPLIER_ORG |
| `dck_order` | `string` | `varchar` |  |  | DCK_ORDER |
| `dck_order_org` | `string` | `varchar` |  |  | DCK_ORDER_ORG |
| `dck_recvdate` | `timestamp` | `timestamp_ntz` |  |  | DCK_RECVDATE |
| `dck_dockid` | `string` | `varchar` |  |  | DCK_DOCKID |
| `dck_location` | `string` | `varchar` |  |  | DCK_LOCATION |
| `dck_shipvia` | `string` | `varchar` |  |  | DCK_SHIPVIA |
| `dck_packslip` | `string` | `varchar` |  |  | DCK_PACKSLIP |
| `dck_freight` | `string` | `varchar` |  |  | DCK_FREIGHT |
| `dck_reference` | `string` | `varchar` |  |  | DCK_REFERENCE |
| `dck_receiver` | `string` | `varchar` |  |  | DCK_RECEIVER |
| `dck_created` | `timestamp` | `timestamp_ntz` |  |  | DCK_CREATED |
| `dck_user` | `string` | `varchar` |  |  | DCK_USER |
| `dck_updated` | `timestamp` | `timestamp_ntz` |  |  | DCK_UPDATED |
| `dck_upduser` | `string` | `varchar` |  |  | DCK_UPDUSER |
| `dck_class` | `string` | `varchar` |  |  | DCK_CLASS |
| `dck_class_org` | `string` | `varchar` |  |  | DCK_CLASS_ORG |
| `dck_updatecount` | `float` | `float` |  |  | DCK_UPDATECOUNT |
| `dck_retrieveall` | `string` | `varchar` |  |  | DCK_RETRIEVEALL |
| `dck_udfchar01` | `string` | `varchar` |  |  | DCK_UDFCHAR01 |
| `dck_udfchar02` | `string` | `varchar` |  |  | DCK_UDFCHAR02 |
| `dck_udfchar03` | `string` | `varchar` |  |  | DCK_UDFCHAR03 |
| `dck_udfchar04` | `string` | `varchar` |  |  | DCK_UDFCHAR04 |
| `dck_udfchar05` | `string` | `varchar` |  |  | DCK_UDFCHAR05 |
| `dck_udfchar06` | `string` | `varchar` |  |  | DCK_UDFCHAR06 |
| `dck_udfchar07` | `string` | `varchar` |  |  | DCK_UDFCHAR07 |
| `dck_udfchar08` | `string` | `varchar` |  |  | DCK_UDFCHAR08 |
| `dck_udfchar09` | `string` | `varchar` |  |  | DCK_UDFCHAR09 |
| `dck_udfchar10` | `string` | `varchar` |  |  | DCK_UDFCHAR10 |
| `dck_udfchar11` | `string` | `varchar` |  |  | DCK_UDFCHAR11 |
| `dck_udfchar12` | `string` | `varchar` |  |  | DCK_UDFCHAR12 |
| `dck_udfchar13` | `string` | `varchar` |  |  | DCK_UDFCHAR13 |
| `dck_udfchar14` | `string` | `varchar` |  |  | DCK_UDFCHAR14 |
| `dck_udfchar15` | `string` | `varchar` |  |  | DCK_UDFCHAR15 |
| `dck_udfchar16` | `string` | `varchar` |  |  | DCK_UDFCHAR16 |
| `dck_udfchar17` | `string` | `varchar` |  |  | DCK_UDFCHAR17 |
| `dck_udfchar18` | `string` | `varchar` |  |  | DCK_UDFCHAR18 |
| `dck_udfchar19` | `string` | `varchar` |  |  | DCK_UDFCHAR19 |
| `dck_udfchar20` | `string` | `varchar` |  |  | DCK_UDFCHAR20 |
| `dck_udfchar21` | `string` | `varchar` |  |  | DCK_UDFCHAR21 |
| `dck_udfchar22` | `string` | `varchar` |  |  | DCK_UDFCHAR22 |
| `dck_udfchar23` | `string` | `varchar` |  |  | DCK_UDFCHAR23 |
| `dck_udfchar24` | `string` | `varchar` |  |  | DCK_UDFCHAR24 |
| `dck_udfchar25` | `string` | `varchar` |  |  | DCK_UDFCHAR25 |
| `dck_udfchar26` | `string` | `varchar` |  |  | DCK_UDFCHAR26 |
| `dck_udfchar27` | `string` | `varchar` |  |  | DCK_UDFCHAR27 |
| `dck_udfchar28` | `string` | `varchar` |  |  | DCK_UDFCHAR28 |
| `dck_udfchar29` | `string` | `varchar` |  |  | DCK_UDFCHAR29 |
| `dck_udfchar30` | `string` | `varchar` |  |  | DCK_UDFCHAR30 |
| `dck_udfnum01` | `float` | `float` |  |  | DCK_UDFNUM01 |
| `dck_udfnum02` | `float` | `float` |  |  | DCK_UDFNUM02 |
| `dck_udfnum03` | `float` | `float` |  |  | DCK_UDFNUM03 |
| `dck_udfnum04` | `float` | `float` |  |  | DCK_UDFNUM04 |
| `dck_udfnum05` | `float` | `float` |  |  | DCK_UDFNUM05 |
| `dck_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | DCK_UDFDATE01 |
| `dck_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | DCK_UDFDATE02 |
| `dck_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | DCK_UDFDATE03 |
| `dck_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | DCK_UDFDATE04 |
| `dck_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | DCK_UDFDATE05 |
| `dck_udfchkbox01` | `string` | `varchar` |  |  | DCK_UDFCHKBOX01 |
| `dck_udfchkbox02` | `string` | `varchar` |  |  | DCK_UDFCHKBOX02 |
| `dck_udfchkbox03` | `string` | `varchar` |  |  | DCK_UDFCHKBOX03 |
| `dck_udfchkbox04` | `string` | `varchar` |  |  | DCK_UDFCHKBOX04 |
| `dck_udfchkbox05` | `string` | `varchar` |  |  | DCK_UDFCHKBOX05 |
| `dck_mrc` | `string` | `varchar` |  |  | DCK_MRC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
