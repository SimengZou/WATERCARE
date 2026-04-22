# r5dlvfunctions

eam_R5DLVFUNCTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dlvfunctions` |
| **Materialization** | `incremental` |
| **Column count** | 92 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fun_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FUN_LASTSAVED |
| `fun_mobileroute` | `string` | `varchar` |  |  | FUN_MOBILEROUTE |
| `fun_saturday` | `string` | `varchar` |  |  | FUN_SATURDAY |
| `fun_descchanged` | `string` | `varchar` |  |  | FUN_DESCCHANGED |
| `fun_class_org` | `string` | `varchar` |  |  | FUN_CLASS_ORG |
| `fun_pertype` | `string` | `varchar` |  |  | FUN_PERTYPE |
| `fun_workbook` | `string` | `varchar` |  |  | FUN_WORKBOOK |
| `fun_worksheet` | `string` | `varchar` |  |  | FUN_WORKSHEET |
| `fun_now` | `string` | `varchar` |  |  | FUN_NOW |
| `fun_rs` | `string` | `varchar` |  |  | FUN_RS |
| `fun_lst` | `string` | `varchar` |  |  | FUN_LST |
| `fun_hdr` | `string` | `varchar` |  |  | FUN_HDR |
| `fun_updatecount` | `float` | `float` |  |  | FUN_UPDATECOUNT |
| `fun_ewsreport` | `string` | `varchar` |  |  | FUN_EWSREPORT |
| `fun_prddatasrc` | `string` | `varchar` |  |  | FUN_PRDDATASRC |
| `fun_rptfileupdatedby` | `string` | `varchar` |  |  | FUN_RPTFILEUPDATEDBY |
| `fun_rptfileupdated` | `timestamp` | `timestamp_ntz` |  |  | FUN_RPTFILEUPDATED |
| `fun_ewsurlnew` | `string` | `varchar` |  |  | FUN_EWSURLNEW |
| `fun_ticket` | `string` | `varchar` |  |  | FUN_TICKET |
| `fun_repdisabled` | `string` | `varchar` |  |  | FUN_REPDISABLED |
| `fun_advrept` | `string` | `varchar` |  |  | FUN_ADVREPT |
| `fun_mekey` | `string` | `varchar` |  |  | FUN_MEKEY |
| `fun_ewsbtn` | `string` | `varchar` |  |  | FUN_EWSBTN |
| `fun_updated` | `timestamp` | `timestamp_ntz` |  |  | FUN_UPDATED |
| `fun_txtcode` | `string` | `varchar` |  |  | FUN_TXTCODE |
| `fun_udfrentity` | `string` | `varchar` |  |  | FUN_UDFRENTITY |
| `fun_fieldfiltertype` | `string` | `varchar` |  |  | FUN_FIELDFILTERTYPE |
| `fun_fieldfilterclass` | `string` | `varchar` |  |  | FUN_FIELDFILTERCLASS |
| `fun_fieldfilterwoobjtype` | `string` | `varchar` |  |  | FUN_FIELDFILTERWOOBJTYPE |
| `fun_xtype` | `string` | `varchar` |  |  | FUN_XTYPE |
| `fun_nodesign` | `string` | `varchar` |  |  | FUN_NODESIGN |
| `fun_startupview` | `string` | `varchar` |  |  | FUN_STARTUPVIEW |
| `fun_fieldfilterchecklist` | `string` | `varchar` |  |  | FUN_FIELDFILTERCHECKLIST |
| `fun_fieldfiltercfclass` | `string` | `varchar` |  |  | FUN_FIELDFILTERCFCLASS |
| `fun_fieldfiltercfclass_org` | `string` | `varchar` |  |  | FUN_FIELDFILTERCFCLASS_ORG |
| `fun_fieldfiltercforg` | `string` | `varchar` |  |  | FUN_FIELDFILTERCFORG |
| `fun_splitviewdisplay` | `string` | `varchar` |  |  | FUN_SPLITVIEWDISPLAY |
| `fun_reservationcalendar` | `string` | `varchar` |  |  | FUN_RESERVATIONCALENDAR |
| `fun_schedulerview` | `string` | `varchar` |  |  | FUN_SCHEDULERVIEW |
| `fun_blockduration` | `string` | `varchar` |  |  | FUN_BLOCKDURATION |
| `fun_workingstarttimehours` | `string` | `varchar` |  |  | FUN_WORKINGSTARTTIMEHOURS |
| `fun_workingstarttimeminutes` | `string` | `varchar` |  |  | FUN_WORKINGSTARTTIMEMINUTES |
| `fun_workingendtimehours` | `string` | `varchar` |  |  | FUN_WORKINGENDTIMEHOURS |
| `fun_workingendtimeminutes` | `string` | `varchar` |  |  | FUN_WORKINGENDTIMEMINUTES |
| `fun_defaultreservationtype` | `string` | `varchar` |  |  | FUN_DEFAULTRESERVATIONTYPE |
| `fun_defaultreservationstatus` | `string` | `varchar` |  |  | FUN_DEFAULTRESERVATIONSTATUS |
| `fun_sunday` | `string` | `varchar` |  |  | FUN_SUNDAY |
| `fun_monday` | `string` | `varchar` |  |  | FUN_MONDAY |
| `fun_tuesday` | `string` | `varchar` |  |  | FUN_TUESDAY |
| `fun_wednesday` | `string` | `varchar` |  |  | FUN_WEDNESDAY |
| `fun_thursday` | `string` | `varchar` |  |  | FUN_THURSDAY |
| `fun_friday` | `string` | `varchar` |  |  | FUN_FRIDAY |
| `fun_autorefreshtimer` | `float` | `float` |  |  | FUN_AUTOREFRESHTIMER |
| `fun_popupfunction` | `string` | `varchar` |  |  | FUN_POPUPFUNCTION |
| `fun_code` | `string` | `varchar` |  |  | FUN_CODE |
| `fun_desc` | `string` | `varchar` |  |  | FUN_DESC |
| `fun_class` | `string` | `varchar` |  |  | FUN_CLASS |
| `fun_subtype` | `string` | `varchar` |  |  | FUN_SUBTYPE |
| `fun_file` | `string` | `varchar` |  |  | FUN_FILE |
| `fun_rentity` | `string` | `varchar` |  |  | FUN_RENTITY |
| `fun_public` | `string` | `varchar` |  |  | FUN_PUBLIC |
| `fun_select` | `string` | `varchar` |  |  | FUN_SELECT |
| `fun_update` | `string` | `varchar` |  |  | FUN_UPDATE |
| `fun_insert` | `string` | `varchar` |  |  | FUN_INSERT |
| `fun_delete` | `string` | `varchar` |  |  | FUN_DELETE |
| `fun_auto` | `string` | `varchar` |  |  | FUN_AUTO |
| `fun_mode` | `string` | `varchar` |  |  | FUN_MODE |
| `fun_device` | `string` | `varchar` |  |  | FUN_DEVICE |
| `fun_orientation` | `string` | `varchar` |  |  | FUN_ORIENTATION |
| `fun_lastvalue` | `string` | `varchar` |  |  | FUN_LASTVALUE |
| `fun_background` | `string` | `varchar` |  |  | FUN_BACKGROUND |
| `fun_application` | `string` | `varchar` |  |  | FUN_APPLICATION |
| `fun_backgroundrep` | `string` | `varchar` |  |  | FUN_BACKGROUNDREP |
| `fun_backgroundgraph` | `string` | `varchar` |  |  | FUN_BACKGROUNDGRAPH |
| `fun_dirsel` | `string` | `varchar` |  |  | FUN_DIRSEL |
| `fun_command` | `string` | `varchar` |  |  | FUN_COMMAND |
| `fun_icon` | `string` | `varchar` |  |  | FUN_ICON |
| `fun_directory` | `string` | `varchar` |  |  | FUN_DIRECTORY |
| `fun_system` | `string` | `varchar` |  |  | FUN_SYSTEM |
| `fun_displayap` | `string` | `varchar` |  |  | FUN_DISPLAYAP |
| `fun_displayft` | `string` | `varchar` |  |  | FUN_DISPLAYFT |
| `fun_popupap` | `string` | `varchar` |  |  | FUN_POPUPAP |
| `fun_popupft` | `string` | `varchar` |  |  | FUN_POPUPFT |
| `fun_parentfunction` | `string` | `varchar` |  |  | FUN_PARENTFUNCTION |
| `fun_parenttab` | `string` | `varchar` |  |  | FUN_PARENTTAB |
| `fun_ugfile` | `string` | `varchar` |  |  | FUN_UGFILE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
