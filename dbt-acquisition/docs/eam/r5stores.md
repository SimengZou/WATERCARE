# r5stores

eam_R5STORES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5stores` |
| **Materialization** | `incremental` |
| **Primary keys** | `str_code` |
| **Column count** | 102 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `str_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | STR_LASTSAVED |
| `str_code` | `string` | `varchar` | `PK` | `unique` | STR_CODE |
| `str_desc` | `string` | `varchar` |  |  | STR_DESC |
| `str_class` | `string` | `varchar` |  |  | STR_CLASS |
| `str_leadtime` | `float` | `float` |  |  | STR_LEADTIME |
| `str_location` | `string` | `varchar` |  |  | STR_LOCATION |
| `str_ltype` | `string` | `varchar` |  |  | STR_LTYPE |
| `str_parent` | `string` | `varchar` |  |  | STR_PARENT |
| `str_pricecode` | `string` | `varchar` |  |  | STR_PRICECODE |
| `str_facility` | `string` | `varchar` |  |  | STR_FACILITY |
| `str_autoappvstatus` | `string` | `varchar` |  |  | STR_AUTOAPPVSTATUS |
| `str_org` | `string` | `varchar` |  |  | STR_ORG |
| `str_copy` | `string` | `varchar` |  |  | STR_COPY |
| `str_class_org` | `string` | `varchar` |  |  | STR_CLASS_ORG |
| `str_location_org` | `string` | `varchar` |  |  | STR_LOCATION_ORG |
| `str_pricetype` | `string` | `varchar` |  |  | STR_PRICETYPE |
| `str_updatecount` | `float` | `float` |  |  | STR_UPDATECOUNT |
| `str_created` | `timestamp` | `timestamp_ntz` |  |  | STR_CREATED |
| `str_updated` | `timestamp` | `timestamp_ntz` |  |  | STR_UPDATED |
| `str_autopostatus` | `string` | `varchar` |  |  | STR_AUTOPOSTATUS |
| `str_reservedpartbuffer` | `float` | `float` |  |  | STR_RESERVEDPARTBUFFER |
| `str_internalleadtime` | `float` | `float` |  |  | STR_INTERNALLEADTIME |
| `str_sunday` | `string` | `varchar` |  |  | STR_SUNDAY |
| `str_monday` | `string` | `varchar` |  |  | STR_MONDAY |
| `str_tuesday` | `string` | `varchar` |  |  | STR_TUESDAY |
| `str_wednesday` | `string` | `varchar` |  |  | STR_WEDNESDAY |
| `str_thursday` | `string` | `varchar` |  |  | STR_THURSDAY |
| `str_friday` | `string` | `varchar` |  |  | STR_FRIDAY |
| `str_saturday` | `string` | `varchar` |  |  | STR_SATURDAY |
| `str_notused` | `string` | `varchar` |  |  | STR_NOTUSED |
| `str_printer` | `string` | `varchar` |  |  | STR_PRINTER |
| `str_printserver` | `string` | `varchar` |  |  | STR_PRINTSERVER |
| `str_issuetemplate` | `string` | `varchar` |  |  | STR_ISSUETEMPLATE |
| `str_receipttemplate` | `string` | `varchar` |  |  | STR_RECEIPTTEMPLATE |
| `str_nonpotemplate` | `string` | `varchar` |  |  | STR_NONPOTEMPLATE |
| `str_segmentvalue` | `string` | `varchar` |  |  | STR_SEGMENTVALUE |
| `str_streceipttemplate` | `string` | `varchar` |  |  | STR_STRECEIPTTEMPLATE |
| `str_enterpriselocation` | `string` | `varchar` |  |  | STR_ENTERPRISELOCATION |
| `str_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | STR_LASTSTATUSUPDATE |
| `str_udfchar01` | `string` | `varchar` |  |  | STR_UDFCHAR01 |
| `str_udfchar02` | `string` | `varchar` |  |  | STR_UDFCHAR02 |
| `str_udfchar03` | `string` | `varchar` |  |  | STR_UDFCHAR03 |
| `str_udfchar04` | `string` | `varchar` |  |  | STR_UDFCHAR04 |
| `str_udfchar05` | `string` | `varchar` |  |  | STR_UDFCHAR05 |
| `str_udfchar06` | `string` | `varchar` |  |  | STR_UDFCHAR06 |
| `str_udfchar07` | `string` | `varchar` |  |  | STR_UDFCHAR07 |
| `str_udfchar08` | `string` | `varchar` |  |  | STR_UDFCHAR08 |
| `str_udfchar09` | `string` | `varchar` |  |  | STR_UDFCHAR09 |
| `str_udfchar10` | `string` | `varchar` |  |  | STR_UDFCHAR10 |
| `str_udfchar11` | `string` | `varchar` |  |  | STR_UDFCHAR11 |
| `str_udfchar12` | `string` | `varchar` |  |  | STR_UDFCHAR12 |
| `str_udfchar13` | `string` | `varchar` |  |  | STR_UDFCHAR13 |
| `str_udfchar14` | `string` | `varchar` |  |  | STR_UDFCHAR14 |
| `str_udfchar15` | `string` | `varchar` |  |  | STR_UDFCHAR15 |
| `str_udfchar16` | `string` | `varchar` |  |  | STR_UDFCHAR16 |
| `str_udfchar17` | `string` | `varchar` |  |  | STR_UDFCHAR17 |
| `str_udfchar18` | `string` | `varchar` |  |  | STR_UDFCHAR18 |
| `str_udfchar19` | `string` | `varchar` |  |  | STR_UDFCHAR19 |
| `str_udfchar20` | `string` | `varchar` |  |  | STR_UDFCHAR20 |
| `str_udfchar21` | `string` | `varchar` |  |  | STR_UDFCHAR21 |
| `str_udfchar22` | `string` | `varchar` |  |  | STR_UDFCHAR22 |
| `str_udfchar23` | `string` | `varchar` |  |  | STR_UDFCHAR23 |
| `str_udfchar24` | `string` | `varchar` |  |  | STR_UDFCHAR24 |
| `str_udfchar25` | `string` | `varchar` |  |  | STR_UDFCHAR25 |
| `str_udfchar26` | `string` | `varchar` |  |  | STR_UDFCHAR26 |
| `str_udfchar27` | `string` | `varchar` |  |  | STR_UDFCHAR27 |
| `str_udfchar28` | `string` | `varchar` |  |  | STR_UDFCHAR28 |
| `str_udfchar29` | `string` | `varchar` |  |  | STR_UDFCHAR29 |
| `str_udfchar30` | `string` | `varchar` |  |  | STR_UDFCHAR30 |
| `str_udfnum01` | `float` | `float` |  |  | STR_UDFNUM01 |
| `str_udfnum02` | `float` | `float` |  |  | STR_UDFNUM02 |
| `str_udfnum03` | `float` | `float` |  |  | STR_UDFNUM03 |
| `str_udfnum04` | `float` | `float` |  |  | STR_UDFNUM04 |
| `str_udfnum05` | `float` | `float` |  |  | STR_UDFNUM05 |
| `str_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | STR_UDFDATE01 |
| `str_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | STR_UDFDATE02 |
| `str_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | STR_UDFDATE03 |
| `str_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | STR_UDFDATE04 |
| `str_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | STR_UDFDATE05 |
| `str_udfchkbox01` | `string` | `varchar` |  |  | STR_UDFCHKBOX01 |
| `str_udfchkbox02` | `string` | `varchar` |  |  | STR_UDFCHKBOX02 |
| `str_udfchkbox03` | `string` | `varchar` |  |  | STR_UDFCHKBOX03 |
| `str_udfchkbox04` | `string` | `varchar` |  |  | STR_UDFCHKBOX04 |
| `str_udfchkbox05` | `string` | `varchar` |  |  | STR_UDFCHKBOX05 |
| `str_tax_parts` | `string` | `varchar` |  |  | STR_TAX_PARTS |
| `str_tax_services` | `string` | `varchar` |  |  | STR_TAX_SERVICES |
| `str_warrantytemplate` | `string` | `varchar` |  |  | STR_WARRANTYTEMPLATE |
| `str_system` | `string` | `varchar` |  |  | STR_SYSTEM |
| `str_system_org` | `string` | `varchar` |  |  | STR_SYSTEM_ORG |
| `str_helditemspotemplate` | `string` | `varchar` |  |  | STR_HELDITEMSPOTEMPLATE |
| `str_helditemsnonpotemplate` | `string` | `varchar` |  |  | STR_HELDITEMSNONPOTEMPLATE |
| `str_helditemsstktrstemplate` | `string` | `varchar` |  |  | STR_HELDITEMSSTKTRSTEMPLATE |
| `str_helditemstrstemplate` | `string` | `varchar` |  |  | STR_HELDITEMSTRSTEMPLATE |
| `str_helditemstostktrstemplate` | `string` | `varchar` |  |  | STR_HELDITEMSTOSTKTRSTEMPLATE |
| `str_helditemsreceipttemplate` | `string` | `varchar` |  |  | STR_HELDITEMSRECEIPTTEMPLATE |
| `str_directtowodefault` | `string` | `varchar` |  |  | STR_DIRECTTOWODEFAULT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
