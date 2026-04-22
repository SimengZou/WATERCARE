# r5docklines

eam_R5DOCKLINES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5docklines` |
| **Materialization** | `incremental` |
| **Primary keys** | `dkl_dckcode`, `dkl_line` |
| **Column count** | 101 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dkl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DKL_LASTSAVED |
| `dkl_udfchar01` | `string` | `varchar` |  |  | DKL_UDFCHAR01 |
| `dkl_udfchar02` | `string` | `varchar` |  |  | DKL_UDFCHAR02 |
| `dkl_udfchar03` | `string` | `varchar` |  |  | DKL_UDFCHAR03 |
| `dkl_udfchar04` | `string` | `varchar` |  |  | DKL_UDFCHAR04 |
| `dkl_udfchar05` | `string` | `varchar` |  |  | DKL_UDFCHAR05 |
| `dkl_udfchar06` | `string` | `varchar` |  |  | DKL_UDFCHAR06 |
| `dkl_udfchar07` | `string` | `varchar` |  |  | DKL_UDFCHAR07 |
| `dkl_udfchar08` | `string` | `varchar` |  |  | DKL_UDFCHAR08 |
| `dkl_udfchar09` | `string` | `varchar` |  |  | DKL_UDFCHAR09 |
| `dkl_udfchar10` | `string` | `varchar` |  |  | DKL_UDFCHAR10 |
| `dkl_udfchar11` | `string` | `varchar` |  |  | DKL_UDFCHAR11 |
| `dkl_udfchar12` | `string` | `varchar` |  |  | DKL_UDFCHAR12 |
| `dkl_udfchar13` | `string` | `varchar` |  |  | DKL_UDFCHAR13 |
| `dkl_udfchar14` | `string` | `varchar` |  |  | DKL_UDFCHAR14 |
| `dkl_udfchar15` | `string` | `varchar` |  |  | DKL_UDFCHAR15 |
| `dkl_udfchar16` | `string` | `varchar` |  |  | DKL_UDFCHAR16 |
| `dkl_udfchar17` | `string` | `varchar` |  |  | DKL_UDFCHAR17 |
| `dkl_udfchar18` | `string` | `varchar` |  |  | DKL_UDFCHAR18 |
| `dkl_udfchar19` | `string` | `varchar` |  |  | DKL_UDFCHAR19 |
| `dkl_udfchar20` | `string` | `varchar` |  |  | DKL_UDFCHAR20 |
| `dkl_udfchar21` | `string` | `varchar` |  |  | DKL_UDFCHAR21 |
| `dkl_udfchar22` | `string` | `varchar` |  |  | DKL_UDFCHAR22 |
| `dkl_udfchar23` | `string` | `varchar` |  |  | DKL_UDFCHAR23 |
| `dkl_udfchar24` | `string` | `varchar` |  |  | DKL_UDFCHAR24 |
| `dkl_udfchar25` | `string` | `varchar` |  |  | DKL_UDFCHAR25 |
| `dkl_udfchar26` | `string` | `varchar` |  |  | DKL_UDFCHAR26 |
| `dkl_udfchar27` | `string` | `varchar` |  |  | DKL_UDFCHAR27 |
| `dkl_udfchar28` | `string` | `varchar` |  |  | DKL_UDFCHAR28 |
| `dkl_udfchar29` | `string` | `varchar` |  |  | DKL_UDFCHAR29 |
| `dkl_udfchar30` | `string` | `varchar` |  |  | DKL_UDFCHAR30 |
| `dkl_udfnum01` | `float` | `float` |  |  | DKL_UDFNUM01 |
| `dkl_udfnum02` | `float` | `float` |  |  | DKL_UDFNUM02 |
| `dkl_udfnum03` | `float` | `float` |  |  | DKL_UDFNUM03 |
| `dkl_udfnum04` | `float` | `float` |  |  | DKL_UDFNUM04 |
| `dkl_udfnum05` | `float` | `float` |  |  | DKL_UDFNUM05 |
| `dkl_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | DKL_UDFDATE01 |
| `dkl_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | DKL_UDFDATE02 |
| `dkl_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | DKL_UDFDATE03 |
| `dkl_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | DKL_UDFDATE04 |
| `dkl_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | DKL_UDFDATE05 |
| `dkl_udfchkbox01` | `string` | `varchar` |  |  | DKL_UDFCHKBOX01 |
| `dkl_udfchkbox02` | `string` | `varchar` |  |  | DKL_UDFCHKBOX02 |
| `dkl_udfchkbox03` | `string` | `varchar` |  |  | DKL_UDFCHKBOX03 |
| `dkl_udfchkbox04` | `string` | `varchar` |  |  | DKL_UDFCHKBOX04 |
| `dkl_udfchkbox05` | `string` | `varchar` |  |  | DKL_UDFCHKBOX05 |
| `dkl_repaircondition` | `string` | `varchar` |  |  | DKL_REPAIRCONDITION |
| `dkl_mobiledateupdated` | `timestamp` | `timestamp_ntz` |  |  | DKL_MOBILEDATEUPDATED |
| `dkl_manufactpart` | `string` | `varchar` |  |  | DKL_MANUFACTPART |
| `dkl_inspect` | `string` | `varchar` |  |  | DKL_INSPECT |
| `dkl_retncode` | `string` | `varchar` |  |  | DKL_RETNCODE |
| `dkl_class` | `string` | `varchar` |  |  | DKL_CLASS |
| `dkl_class_org` | `string` | `varchar` |  |  | DKL_CLASS_ORG |
| `dkl_scrapqty` | `float` | `float` |  |  | DKL_SCRAPQTY |
| `dkl_acd` | `float` | `float` |  |  | DKL_ACD |
| `dkl_updatecount` | `float` | `float` |  |  | DKL_UPDATECOUNT |
| `dkl_repairprice` | `float` | `float` |  |  | DKL_REPAIRPRICE |
| `dkl_person` | `string` | `varchar` |  |  | DKL_PERSON |
| `dkl_manufacturer` | `string` | `varchar` |  |  | DKL_MANUFACTURER |
| `dkl_line` | `float` | `float` | `PK` |  | DKL_LINE |
| `dkl_order` | `string` | `varchar` |  |  | DKL_ORDER |
| `dkl_ordline` | `float` | `float` |  |  | DKL_ORDLINE |
| `dkl_order_org` | `string` | `varchar` |  |  | DKL_ORDER_ORG |
| `dkl_part` | `string` | `varchar` |  |  | DKL_PART |
| `dkl_part_org` | `string` | `varchar` |  |  | DKL_PART_ORG |
| `dkl_location` | `string` | `varchar` |  |  | DKL_LOCATION |
| `dkl_inspstatus` | `string` | `varchar` |  |  | DKL_INSPSTATUS |
| `dkl_insprstatus` | `string` | `varchar` |  |  | DKL_INSPRSTATUS |
| `dkl_linestatus` | `string` | `varchar` |  |  | DKL_LINESTATUS |
| `dkl_linerstatus` | `string` | `varchar` |  |  | DKL_LINERSTATUS |
| `dkl_counted` | `string` | `varchar` |  |  | DKL_COUNTED |
| `dkl_recvqty` | `float` | `float` |  |  | DKL_RECVQTY |
| `dkl_countqty` | `float` | `float` |  |  | DKL_COUNTQTY |
| `dkl_print` | `float` | `float` |  |  | DKL_PRINT |
| `dkl_inspectedqty` | `float` | `float` |  |  | DKL_INSPECTEDQTY |
| `dkl_insprejqty` | `float` | `float` |  |  | DKL_INSPREJQTY |
| `dkl_rejreason` | `string` | `varchar` |  |  | DKL_REJREASON |
| `dkl_inspector` | `string` | `varchar` |  |  | DKL_INSPECTOR |
| `dkl_inspdate` | `timestamp` | `timestamp_ntz` |  |  | DKL_INSPDATE |
| `dkl_upduser` | `string` | `varchar` |  |  | DKL_UPDUSER |
| `dkl_updated` | `timestamp` | `timestamp_ntz` |  |  | DKL_UPDATED |
| `dkl_retnqty` | `float` | `float` |  |  | DKL_RETNQTY |
| `dkl_bin` | `string` | `varchar` |  |  | DKL_BIN |
| `dkl_lot` | `string` | `varchar` |  |  | DKL_LOT |
| `dkl_manlot` | `string` | `varchar` |  |  | DKL_MANLOT |
| `dkl_manlotexp` | `timestamp` | `timestamp_ntz` |  |  | DKL_MANLOTEXP |
| `dkl_direct` | `string` | `varchar` |  |  | DKL_DIRECT |
| `dkl_obtype` | `string` | `varchar` |  |  | DKL_OBTYPE |
| `dkl_obrtype` | `string` | `varchar` |  |  | DKL_OBRTYPE |
| `dkl_object` | `string` | `varchar` |  |  | DKL_OBJECT |
| `dkl_dckcode` | `string` | `varchar` | `PK` |  | DKL_DCKCODE |
| `dkl_object_org` | `string` | `varchar` |  |  | DKL_OBJECT_ORG |
| `dkl_serial` | `string` | `varchar` |  |  | DKL_SERIAL |
| `dkl_mrc` | `string` | `varchar` |  |  | DKL_MRC |
| `dkl_recvcode` | `string` | `varchar` |  |  | DKL_RECVCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
