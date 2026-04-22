# r5documents

eam_R5DOCUMENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5documents` |
| **Materialization** | `incremental` |
| **Primary keys** | `doc_code` |
| **Column count** | 112 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `doc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DOC_LASTSAVED |
| `doc_code` | `string` | `varchar` | `PK` | `unique` | DOC_CODE |
| `doc_desc` | `string` | `varchar` |  |  | DOC_DESC |
| `doc_class` | `string` | `varchar` |  |  | DOC_CLASS |
| `doc_origcode` | `string` | `varchar` |  |  | DOC_ORIGCODE |
| `doc_revnumber` | `string` | `varchar` |  |  | DOC_REVNUMBER |
| `doc_revision` | `timestamp` | `timestamp_ntz` |  |  | DOC_REVISION |
| `doc_parent` | `string` | `varchar` |  |  | DOC_PARENT |
| `doc_origin` | `string` | `varchar` |  |  | DOC_ORIGIN |
| `doc_pages` | `float` | `float` |  |  | DOC_PAGES |
| `doc_size` | `string` | `varchar` |  |  | DOC_SIZE |
| `doc_cabinet` | `string` | `varchar` |  |  | DOC_CABINET |
| `doc_filename` | `string` | `varchar` |  |  | DOC_FILENAME |
| `doc_filetype` | `string` | `varchar` |  |  | DOC_FILETYPE |
| `doc_expir` | `timestamp` | `timestamp_ntz` |  |  | DOC_EXPIR |
| `doc_supplier` | `string` | `varchar` |  |  | DOC_SUPPLIER |
| `doc_warstart` | `timestamp` | `timestamp_ntz` |  |  | DOC_WARSTART |
| `doc_warranty` | `string` | `varchar` |  |  | DOC_WARRANTY |
| `doc_duration` | `float` | `float` |  |  | DOC_DURATION |
| `doc_threshold` | `float` | `float` |  |  | DOC_THRESHOLD |
| `doc_manufacturer` | `string` | `varchar` |  |  | DOC_MANUFACTURER |
| `doc_org` | `string` | `varchar` |  |  | DOC_ORG |
| `doc_class_org` | `string` | `varchar` |  |  | DOC_CLASS_ORG |
| `doc_supplier_org` | `string` | `varchar` |  |  | DOC_SUPPLIER_ORG |
| `doc_type` | `string` | `varchar` |  |  | DOC_TYPE |
| `doc_rtype` | `string` | `varchar` |  |  | DOC_RTYPE |
| `doc_filesize` | `float` | `float` |  |  | DOC_FILESIZE |
| `doc_lockuser` | `string` | `varchar` |  |  | DOC_LOCKUSER |
| `doc_updatecount` | `float` | `float` |  |  | DOC_UPDATECOUNT |
| `doc_labcharge` | `float` | `float` |  |  | DOC_LABCHARGE |
| `doc_matcharge` | `float` | `float` |  |  | DOC_MATCHARGE |
| `doc_hircharge` | `float` | `float` |  |  | DOC_HIRCHARGE |
| `doc_dmacharge` | `float` | `float` |  |  | DOC_DMACHARGE |
| `doc_servicecharge` | `float` | `float` |  |  | DOC_SERVICECHARGE |
| `doc_toolcharge` | `float` | `float` |  |  | DOC_TOOLCHARGE |
| `doc_notused` | `string` | `varchar` |  |  | DOC_NOTUSED |
| `doc_origfilename` | `string` | `varchar` |  |  | DOC_ORIGFILENAME |
| `doc_uploaded` | `timestamp` | `timestamp_ntz` |  |  | DOC_UPLOADED |
| `doc_warrantytype` | `string` | `varchar` |  |  | DOC_WARRANTYTYPE |
| `doc_controlnumber` | `string` | `varchar` |  |  | DOC_CONTROLNUMBER |
| `doc_agreementtype` | `string` | `varchar` |  |  | DOC_AGREEMENTTYPE |
| `doc_onsiterepair` | `string` | `varchar` |  |  | DOC_ONSITEREPAIR |
| `doc_loanerprovided` | `string` | `varchar` |  |  | DOC_LOANERPROVIDED |
| `doc_renewalalertemail` | `string` | `varchar` |  |  | DOC_RENEWALALERTEMAIL |
| `doc_startdatebasis` | `string` | `varchar` |  |  | DOC_STARTDATEBASIS |
| `doc_fixedlaborrate` | `float` | `float` |  |  | DOC_FIXEDLABORRATE |
| `doc_fixedlaboramount` | `float` | `float` |  |  | DOC_FIXEDLABORAMOUNT |
| `doc_maxlaboramount` | `float` | `float` |  |  | DOC_MAXLABORAMOUNT |
| `doc_maxstockamount` | `float` | `float` |  |  | DOC_MAXSTOCKAMOUNT |
| `doc_excludepmworkorder` | `string` | `varchar` |  |  | DOC_EXCLUDEPMWORKORDER |
| `doc_partlistrequired` | `string` | `varchar` |  |  | DOC_PARTLISTREQUIRED |
| `doc_rmarequired` | `string` | `varchar` |  |  | DOC_RMAREQUIRED |
| `doc_udfchar01` | `string` | `varchar` |  |  | DOC_UDFCHAR01 |
| `doc_udfchar02` | `string` | `varchar` |  |  | DOC_UDFCHAR02 |
| `doc_udfchar03` | `string` | `varchar` |  |  | DOC_UDFCHAR03 |
| `doc_udfchar04` | `string` | `varchar` |  |  | DOC_UDFCHAR04 |
| `doc_udfchar05` | `string` | `varchar` |  |  | DOC_UDFCHAR05 |
| `doc_udfchar06` | `string` | `varchar` |  |  | DOC_UDFCHAR06 |
| `doc_udfchar07` | `string` | `varchar` |  |  | DOC_UDFCHAR07 |
| `doc_udfchar08` | `string` | `varchar` |  |  | DOC_UDFCHAR08 |
| `doc_udfchar09` | `string` | `varchar` |  |  | DOC_UDFCHAR09 |
| `doc_udfchar10` | `string` | `varchar` |  |  | DOC_UDFCHAR10 |
| `doc_udfchar11` | `string` | `varchar` |  |  | DOC_UDFCHAR11 |
| `doc_udfchar12` | `string` | `varchar` |  |  | DOC_UDFCHAR12 |
| `doc_udfchar13` | `string` | `varchar` |  |  | DOC_UDFCHAR13 |
| `doc_udfchar14` | `string` | `varchar` |  |  | DOC_UDFCHAR14 |
| `doc_udfchar15` | `string` | `varchar` |  |  | DOC_UDFCHAR15 |
| `doc_udfchar16` | `string` | `varchar` |  |  | DOC_UDFCHAR16 |
| `doc_udfchar17` | `string` | `varchar` |  |  | DOC_UDFCHAR17 |
| `doc_udfchar18` | `string` | `varchar` |  |  | DOC_UDFCHAR18 |
| `doc_udfchar19` | `string` | `varchar` |  |  | DOC_UDFCHAR19 |
| `doc_udfchar20` | `string` | `varchar` |  |  | DOC_UDFCHAR20 |
| `doc_udfchar21` | `string` | `varchar` |  |  | DOC_UDFCHAR21 |
| `doc_udfchar22` | `string` | `varchar` |  |  | DOC_UDFCHAR22 |
| `doc_udfchar23` | `string` | `varchar` |  |  | DOC_UDFCHAR23 |
| `doc_udfchar24` | `string` | `varchar` |  |  | DOC_UDFCHAR24 |
| `doc_udfchar25` | `string` | `varchar` |  |  | DOC_UDFCHAR25 |
| `doc_udfchar26` | `string` | `varchar` |  |  | DOC_UDFCHAR26 |
| `doc_udfchar27` | `string` | `varchar` |  |  | DOC_UDFCHAR27 |
| `doc_udfchar28` | `string` | `varchar` |  |  | DOC_UDFCHAR28 |
| `doc_udfchar29` | `string` | `varchar` |  |  | DOC_UDFCHAR29 |
| `doc_udfchar30` | `string` | `varchar` |  |  | DOC_UDFCHAR30 |
| `doc_udfnum01` | `float` | `float` |  |  | DOC_UDFNUM01 |
| `doc_udfnum02` | `float` | `float` |  |  | DOC_UDFNUM02 |
| `doc_udfnum03` | `float` | `float` |  |  | DOC_UDFNUM03 |
| `doc_fixedstockamount` | `float` | `float` |  |  | DOC_FIXEDSTOCKAMOUNT |
| `doc_udfnum04` | `float` | `float` |  |  | DOC_UDFNUM04 |
| `doc_udfnum05` | `float` | `float` |  |  | DOC_UDFNUM05 |
| `doc_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | DOC_UDFDATE01 |
| `doc_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | DOC_UDFDATE02 |
| `doc_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | DOC_UDFDATE03 |
| `doc_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | DOC_UDFDATE04 |
| `doc_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | DOC_UDFDATE05 |
| `doc_udfchkbox01` | `string` | `varchar` |  |  | DOC_UDFCHKBOX01 |
| `doc_udfchkbox02` | `string` | `varchar` |  |  | DOC_UDFCHKBOX02 |
| `doc_udfchkbox03` | `string` | `varchar` |  |  | DOC_UDFCHKBOX03 |
| `doc_udfchkbox04` | `string` | `varchar` |  |  | DOC_UDFCHKBOX04 |
| `doc_udfchkbox05` | `string` | `varchar` |  |  | DOC_UDFCHKBOX05 |
| `doc_content` | `string` | `varchar` |  |  | DOC_CONTENT. Max length: 0 |
| `doc_idmtype` | `string` | `varchar` |  |  | DOC_IDMTYPE |
| `doc_title` | `string` | `varchar` |  |  | DOC_TITLE |
| `doc_dateeffective` | `timestamp` | `timestamp_ntz` |  |  | DOC_DATEEFFECTIVE |
| `doc_dateexpired` | `timestamp` | `timestamp_ntz` |  |  | DOC_DATEEXPIRED |
| `doc_isprofilepicture` | `string` | `varchar` |  |  | DOC_ISPROFILEPICTURE |
| `doc_ismobilelog` | `string` | `varchar` |  |  | DOC_ISMOBILELOG |
| `doc_isimage` | `string` | `varchar` |  |  | DOC_ISIMAGE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
