# r5activities

eam_R5ACTIVITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5activities` |
| **Materialization** | `incremental` |
| **Primary keys** | `act_event`, `act_act` |
| **Column count** | 194 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `act_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACT_LASTSAVED |
| `act_event` | `string` | `varchar` | `PK` |  | ACT_EVENT |
| `act_act` | `float` | `float` | `PK` |  | ACT_ACT |
| `act_supplier` | `string` | `varchar` |  |  | ACT_SUPPLIER |
| `act_start` | `timestamp` | `timestamp_ntz` |  |  | ACT_START |
| `act_time` | `float` | `float` |  |  | ACT_TIME |
| `act_newstart` | `timestamp` | `timestamp_ntz` |  |  | ACT_NEWSTART |
| `act_hire` | `string` | `varchar` |  |  | ACT_HIRE |
| `act_ordered` | `string` | `varchar` |  |  | ACT_ORDERED |
| `act_fixh` | `string` | `varchar` |  |  | ACT_FIXH |
| `act_minhours` | `string` | `varchar` |  |  | ACT_MINHOURS |
| `act_mrc` | `string` | `varchar` |  |  | ACT_MRC |
| `act_trade` | `string` | `varchar` |  |  | ACT_TRADE |
| `act_shift` | `string` | `varchar` |  |  | ACT_SHIFT |
| `act_persons` | `float` | `float` |  |  | ACT_PERSONS |
| `act_project` | `string` | `varchar` |  |  | ACT_PROJECT |
| `act_projbud` | `string` | `varchar` |  |  | ACT_PROJBUD |
| `act_route` | `string` | `varchar` |  |  | ACT_ROUTE |
| `act_duration` | `float` | `float` |  |  | ACT_DURATION |
| `act_newdur` | `float` | `float` |  |  | ACT_NEWDUR |
| `act_maxdur` | `float` | `float` |  |  | ACT_MAXDUR |
| `act_est` | `float` | `float` |  |  | ACT_EST |
| `act_rem` | `float` | `float` |  |  | ACT_REM |
| `act_nt` | `float` | `float` |  |  | ACT_NT |
| `act_ntrate` | `float` | `float` |  |  | ACT_NTRATE |
| `act_ot` | `float` | `float` |  |  | ACT_OT |
| `act_otrate` | `float` | `float` |  |  | ACT_OTRATE |
| `act_purrate` | `float` | `float` |  |  | ACT_PURRATE |
| `act_task` | `string` | `varchar` |  |  | ACT_TASK |
| `act_matlist` | `string` | `varchar` |  |  | ACT_MATLIST |
| `act_special` | `string` | `varchar` |  |  | ACT_SPECIAL |
| `act_graph` | `string` | `varchar` |  |  | ACT_GRAPH |
| `act_level` | `float` | `float` |  |  | ACT_LEVEL |
| `act_order` | `string` | `varchar` |  |  | ACT_ORDER |
| `act_ordline` | `float` | `float` |  |  | ACT_ORDLINE |
| `act_ordtype` | `string` | `varchar` |  |  | ACT_ORDTYPE |
| `act_ordrtype` | `string` | `varchar` |  |  | ACT_ORDRTYPE |
| `act_schedhrs` | `float` | `float` |  |  | ACT_SCHEDHRS |
| `act_latestsched` | `timestamp` | `timestamp_ntz` |  |  | ACT_LATESTSCHED |
| `act_completed` | `string` | `varchar` |  |  | ACT_COMPLETED |
| `act_matlrev` | `float` | `float` |  |  | ACT_MATLREV |
| `act_taskrev` | `float` | `float` |  |  | ACT_TASKREV |
| `act_qty` | `float` | `float` |  |  | ACT_QTY |
| `act_uom` | `string` | `varchar` |  |  | ACT_UOM |
| `act_req` | `string` | `varchar` |  |  | ACT_REQ |
| `act_reqline` | `float` | `float` |  |  | ACT_REQLINE |
| `act_supplier_org` | `string` | `varchar` |  |  | ACT_SUPPLIER_ORG |
| `act_order_org` | `string` | `varchar` |  |  | ACT_ORDER_ORG |
| `act_rpc` | `string` | `varchar` |  |  | ACT_RPC |
| `act_wap` | `string` | `varchar` |  |  | ACT_WAP |
| `act_tpf` | `string` | `varchar` |  |  | ACT_TPF |
| `act_manufact` | `string` | `varchar` |  |  | ACT_MANUFACT |
| `act_syslevel` | `string` | `varchar` |  |  | ACT_SYSLEVEL |
| `act_asmlevel` | `string` | `varchar` |  |  | ACT_ASMLEVEL |
| `act_complevel` | `string` | `varchar` |  |  | ACT_COMPLEVEL |
| `act_class` | `string` | `varchar` |  |  | ACT_CLASS |
| `act_class_org` | `string` | `varchar` |  |  | ACT_CLASS_ORG |
| `act_percomplete` | `float` | `float` |  |  | ACT_PERCOMPLETE |
| `act_updatecount` | `float` | `float` |  |  | ACT_UPDATECOUNT |
| `act_sourcecode` | `string` | `varchar` |  |  | ACT_SOURCECODE |
| `act_sourcesystem` | `string` | `varchar` |  |  | ACT_SOURCESYSTEM |
| `act_perresp` | `string` | `varchar` |  |  | ACT_PERRESP |
| `act_warranty` | `string` | `varchar` |  |  | ACT_WARRANTY |
| `act_relatedwo` | `string` | `varchar` |  |  | ACT_RELATEDWO |
| `act_note` | `string` | `varchar` |  |  | ACT_NOTE |
| `act_defermaintenance` | `string` | `varchar` |  |  | ACT_DEFERMAINTENANCE |
| `act_deferredirectmats` | `string` | `varchar` |  |  | ACT_DEFERREDIRECTMATS |
| `act_deferredorigwo` | `string` | `varchar` |  |  | ACT_DEFERREDORIGWO |
| `act_deferredorigact` | `float` | `float` |  |  | ACT_DEFERREDORIGACT |
| `act_udfchar01` | `string` | `varchar` |  |  | ACT_UDFCHAR01 |
| `act_udfchar02` | `string` | `varchar` |  |  | ACT_UDFCHAR02 |
| `act_udfchar03` | `string` | `varchar` |  |  | ACT_UDFCHAR03 |
| `act_udfchar04` | `string` | `varchar` |  |  | ACT_UDFCHAR04 |
| `act_udfchar05` | `string` | `varchar` |  |  | ACT_UDFCHAR05 |
| `act_udfchar06` | `string` | `varchar` |  |  | ACT_UDFCHAR06 |
| `act_udfchar07` | `string` | `varchar` |  |  | ACT_UDFCHAR07 |
| `act_udfchar08` | `string` | `varchar` |  |  | ACT_UDFCHAR08 |
| `act_udfchar09` | `string` | `varchar` |  |  | ACT_UDFCHAR09 |
| `act_udfchar10` | `string` | `varchar` |  |  | ACT_UDFCHAR10 |
| `act_udfchar11` | `string` | `varchar` |  |  | ACT_UDFCHAR11 |
| `act_udfchar12` | `string` | `varchar` |  |  | ACT_UDFCHAR12 |
| `act_udfchar13` | `string` | `varchar` |  |  | ACT_UDFCHAR13 |
| `act_udfchar14` | `string` | `varchar` |  |  | ACT_UDFCHAR14 |
| `act_udfchar15` | `string` | `varchar` |  |  | ACT_UDFCHAR15 |
| `act_udfchar16` | `string` | `varchar` |  |  | ACT_UDFCHAR16 |
| `act_udfchar17` | `string` | `varchar` |  |  | ACT_UDFCHAR17 |
| `act_udfchar18` | `string` | `varchar` |  |  | ACT_UDFCHAR18 |
| `act_udfchar19` | `string` | `varchar` |  |  | ACT_UDFCHAR19 |
| `act_udfchar20` | `string` | `varchar` |  |  | ACT_UDFCHAR20 |
| `act_udfchar21` | `string` | `varchar` |  |  | ACT_UDFCHAR21 |
| `act_udfchar22` | `string` | `varchar` |  |  | ACT_UDFCHAR22 |
| `act_udfchar23` | `string` | `varchar` |  |  | ACT_UDFCHAR23 |
| `act_udfchar24` | `string` | `varchar` |  |  | ACT_UDFCHAR24 |
| `act_udfchar25` | `string` | `varchar` |  |  | ACT_UDFCHAR25 |
| `act_udfchar26` | `string` | `varchar` |  |  | ACT_UDFCHAR26 |
| `act_udfchar27` | `string` | `varchar` |  |  | ACT_UDFCHAR27 |
| `act_udfchar28` | `string` | `varchar` |  |  | ACT_UDFCHAR28 |
| `act_udfchar29` | `string` | `varchar` |  |  | ACT_UDFCHAR29 |
| `act_udfchar30` | `string` | `varchar` |  |  | ACT_UDFCHAR30 |
| `act_udfnum01` | `float` | `float` |  |  | ACT_UDFNUM01 |
| `act_udfnum02` | `float` | `float` |  |  | ACT_UDFNUM02 |
| `act_udfnum03` | `float` | `float` |  |  | ACT_UDFNUM03 |
| `act_udfnum04` | `float` | `float` |  |  | ACT_UDFNUM04 |
| `act_udfnum05` | `float` | `float` |  |  | ACT_UDFNUM05 |
| `act_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ACT_UDFDATE01 |
| `act_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ACT_UDFDATE02 |
| `act_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ACT_UDFDATE03 |
| `act_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ACT_UDFDATE04 |
| `act_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ACT_UDFDATE05 |
| `act_udfchkbox01` | `string` | `varchar` |  |  | ACT_UDFCHKBOX01 |
| `act_udfchkbox02` | `string` | `varchar` |  |  | ACT_UDFCHKBOX02 |
| `act_udfchkbox03` | `string` | `varchar` |  |  | ACT_UDFCHKBOX03 |
| `act_udfchkbox04` | `string` | `varchar` |  |  | ACT_UDFCHKBOX04 |
| `act_udfchkbox05` | `string` | `varchar` |  |  | ACT_UDFCHKBOX05 |
| `act_invoice` | `string` | `varchar` |  |  | ACT_INVOICE |
| `act_invoice_org` | `string` | `varchar` |  |  | ACT_INVOICE_ORG |
| `act_invoice_revision` | `float` | `float` |  |  | ACT_INVOICE_REVISION |
| `act_invoiceline` | `float` | `float` |  |  | ACT_INVOICELINE |
| `act_performed` | `timestamp` | `timestamp_ntz` |  |  | ACT_PERFORMED |
| `act_performedby` | `string` | `varchar` |  |  | ACT_PERFORMEDBY |
| `act_performedtype` | `string` | `varchar` |  |  | ACT_PERFORMEDTYPE |
| `act_reviewed` | `timestamp` | `timestamp_ntz` |  |  | ACT_REVIEWED |
| `act_reviewedby` | `string` | `varchar` |  |  | ACT_REVIEWEDBY |
| `act_reviewedtype` | `string` | `varchar` |  |  | ACT_REVIEWEDTYPE |
| `act_complocation` | `string` | `varchar` |  |  | ACT_COMPLOCATION |
| `act_schedulingsessionid` | `float` | `float` |  |  | ACT_SCHEDULINGSESSIONID |
| `act_schedulingsessiontype` | `string` | `varchar` |  |  | ACT_SCHEDULINGSESSIONTYPE |
| `act_topparent` | `float` | `float` |  |  | ACT_TOPPARENT |
| `act_parent` | `float` | `float` |  |  | ACT_PARENT |
| `act_seq` | `float` | `float` |  |  | ACT_SEQ |
| `act_planninglevel` | `string` | `varchar` |  |  | ACT_PLANNINGLEVEL |
| `act_multipletrades` | `string` | `varchar` |  |  | ACT_MULTIPLETRADES |
| `act_frompoint` | `float` | `float` |  |  | ACT_FROMPOINT |
| `act_fromrefdesc` | `string` | `varchar` |  |  | ACT_FROMREFDESC |
| `act_fromgeoref` | `string` | `varchar` |  |  | ACT_FROMGEOREF |
| `act_topoint` | `float` | `float` |  |  | ACT_TOPOINT |
| `act_torefdesc` | `string` | `varchar` |  |  | ACT_TOREFDESC |
| `act_togeoref` | `string` | `varchar` |  |  | ACT_TOGEOREF |
| `act_estlaborcost` | `float` | `float` |  |  | ACT_ESTLABORCOST |
| `act_estmatlcost` | `float` | `float` |  |  | ACT_ESTMATLCOST |
| `act_estmisccost` | `float` | `float` |  |  | ACT_ESTMISCCOST |
| `act_assignmentstatus` | `string` | `varchar` |  |  | ACT_ASSIGNMENTSTATUS |
| `act_from_reference` | `float` | `float` |  |  | ACT_FROM_REFERENCE |
| `act_from_offset` | `float` | `float` |  |  | ACT_FROM_OFFSET |
| `act_from_offset_percentage` | `float` | `float` |  |  | ACT_FROM_OFFSET_PERCENTAGE |
| `act_from_offset_direction` | `string` | `varchar` |  |  | ACT_FROM_OFFSET_DIRECTION |
| `act_from_xcoordinate` | `float` | `float` |  |  | ACT_FROM_XCOORDINATE |
| `act_from_ycoordinate` | `float` | `float` |  |  | ACT_FROM_YCOORDINATE |
| `act_from_latitude` | `float` | `float` |  |  | ACT_FROM_LATITUDE |
| `act_from_longitude` | `float` | `float` |  |  | ACT_FROM_LONGITUDE |
| `act_from_relationship_type` | `string` | `varchar` |  |  | ACT_FROM_RELATIONSHIP_TYPE |
| `act_from_horizontal_offset` | `float` | `float` |  |  | ACT_FROM_HORIZONTAL_OFFSET |
| `act_from_horizontal_offsetuom` | `string` | `varchar` |  |  | ACT_FROM_HORIZONTAL_OFFSETUOM |
| `act_from_horizontal_offsettype` | `string` | `varchar` |  |  | ACT_FROM_HORIZONTAL_OFFSETTYPE |
| `act_from_vertical_offset` | `float` | `float` |  |  | ACT_FROM_VERTICAL_OFFSET |
| `act_from_vertical_offsetuom` | `string` | `varchar` |  |  | ACT_FROM_VERTICAL_OFFSETUOM |
| `act_from_vertical_offsettype` | `string` | `varchar` |  |  | ACT_FROM_VERTICAL_OFFSETTYPE |
| `act_to_reference` | `float` | `float` |  |  | ACT_TO_REFERENCE |
| `act_to_offset` | `float` | `float` |  |  | ACT_TO_OFFSET |
| `act_to_offset_direction` | `string` | `varchar` |  |  | ACT_TO_OFFSET_DIRECTION |
| `act_to_offset_percentage` | `float` | `float` |  |  | ACT_TO_OFFSET_PERCENTAGE |
| `act_to_xcoordinate` | `float` | `float` |  |  | ACT_TO_XCOORDINATE |
| `act_to_ycoordinate` | `float` | `float` |  |  | ACT_TO_YCOORDINATE |
| `act_to_latitude` | `float` | `float` |  |  | ACT_TO_LATITUDE |
| `act_to_longitude` | `float` | `float` |  |  | ACT_TO_LONGITUDE |
| `act_to_relationship_type` | `string` | `varchar` |  |  | ACT_TO_RELATIONSHIP_TYPE |
| `act_to_vertical_offset` | `float` | `float` |  |  | ACT_TO_VERTICAL_OFFSET |
| `act_to_horizontal_offset` | `float` | `float` |  |  | ACT_TO_HORIZONTAL_OFFSET |
| `act_to_horizontal_offsetuom` | `string` | `varchar` |  |  | ACT_TO_HORIZONTAL_OFFSETUOM |
| `act_to_horizontal_offsettype` | `string` | `varchar` |  |  | ACT_TO_HORIZONTAL_OFFSETTYPE |
| `act_to_vertical_offsetuom` | `string` | `varchar` |  |  | ACT_TO_VERTICAL_OFFSETUOM |
| `act_to_vertical_offsettype` | `string` | `varchar` |  |  | ACT_TO_VERTICAL_OFFSETTYPE |
| `act_performed2` | `timestamp` | `timestamp_ntz` |  |  | ACT_PERFORMED2 |
| `act_performedby2` | `string` | `varchar` |  |  | ACT_PERFORMEDBY2 |
| `act_performedtype2` | `string` | `varchar` |  |  | ACT_PERFORMEDTYPE2 |
| `act_rejectperformedby` | `string` | `varchar` |  |  | ACT_REJECTPERFORMEDBY |
| `act_rejectperformedby2` | `string` | `varchar` |  |  | ACT_REJECTPERFORMEDBY2 |
| `act_rejectreason` | `string` | `varchar` |  |  | ACT_REJECTREASON |
| `act_condition_as_found` | `string` | `varchar` |  |  | ACT_CONDITION_AS_FOUND |
| `act_reviewresp` | `string` | `varchar` |  |  | ACT_REVIEWRESP |
| `act_performbyresp` | `string` | `varchar` |  |  | ACT_PERFORMBYRESP |
| `act_performby2resp` | `string` | `varchar` |  |  | ACT_PERFORMBY2RESP |
| `act_udfnote01` | `string` | `varchar` |  |  | ACT_UDFNOTE01 |
| `act_udfnote02` | `string` | `varchar` |  |  | ACT_UDFNOTE02 |
| `act_recordedby` | `string` | `varchar` |  |  | ACT_RECORDEDBY |
| `act_daterecorded` | `timestamp` | `timestamp_ntz` |  |  | ACT_DATERECORDED |
| `act_dispatcherrors` | `string` | `varchar` |  |  | ACT_DISPATCHERRORS |
| `act_cuest` | `string` | `varchar` |  |  | ACT_CUEST |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
