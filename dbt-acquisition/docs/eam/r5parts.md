# r5parts

eam_R5PARTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5parts` |
| **Materialization** | `incremental` |
| **Primary keys** | `par_code`, `par_org` |
| **Column count** | 119 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `par_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PAR_LASTSAVED |
| `par_code` | `string` | `varchar` | `PK` |  | PAR_CODE |
| `par_desc` | `string` | `varchar` |  |  | PAR_DESC |
| `par_class` | `string` | `varchar` |  |  | PAR_CLASS |
| `par_uom` | `string` | `varchar` |  |  | PAR_UOM |
| `par_byasset` | `string` | `varchar` |  |  | PAR_BYASSET |
| `par_bylot` | `string` | `varchar` |  |  | PAR_BYLOT |
| `par_category` | `string` | `varchar` |  |  | PAR_CATEGORY |
| `par_baseprice` | `float` | `float` |  |  | PAR_BASEPRICE |
| `par_avgprice` | `float` | `float` |  |  | PAR_AVGPRICE |
| `par_lastprice` | `float` | `float` |  |  | PAR_LASTPRICE |
| `par_stdprice` | `float` | `float` |  |  | PAR_STDPRICE |
| `par_prefsup` | `string` | `varchar` |  |  | PAR_PREFSUP |
| `par_prefsupprice` | `float` | `float` |  |  | PAR_PREFSUPPRICE |
| `par_prefuom` | `string` | `varchar` |  |  | PAR_PREFUOM |
| `par_commodity` | `string` | `varchar` |  |  | PAR_COMMODITY |
| `par_subcommodity` | `string` | `varchar` |  |  | PAR_SUBCOMMODITY |
| `par_tax` | `string` | `varchar` |  |  | PAR_TAX |
| `par_buyer` | `string` | `varchar` |  |  | PAR_BUYER |
| `par_sourcesystem` | `string` | `varchar` |  |  | PAR_SOURCESYSTEM |
| `par_sourcecode` | `string` | `varchar` |  |  | PAR_SOURCECODE |
| `par_interface` | `timestamp` | `timestamp_ntz` |  |  | PAR_INTERFACE |
| `par_notused` | `string` | `varchar` |  |  | PAR_NOTUSED |
| `par_tracktype` | `string` | `varchar` |  |  | PAR_TRACKTYPE |
| `par_trackrtype` | `string` | `varchar` |  |  | PAR_TRACKRTYPE |
| `par_tool` | `string` | `varchar` |  |  | PAR_TOOL |
| `par_wardays` | `float` | `float` |  |  | PAR_WARDAYS |
| `par_org` | `string` | `varchar` | `PK` |  | PAR_ORG |
| `par_class_org` | `string` | `varchar` |  |  | PAR_CLASS_ORG |
| `par_prefsup_org` | `string` | `varchar` |  |  | PAR_PREFSUP_ORG |
| `par_inspect` | `string` | `varchar` |  |  | PAR_INSPECT |
| `par_insmethod` | `string` | `varchar` |  |  | PAR_INSMETHOD |
| `par_codestructure` | `string` | `varchar` |  |  | PAR_CODESTRUCTURE |
| `par_corevalue` | `float` | `float` |  |  | PAR_COREVALUE |
| `par_repairable` | `string` | `varchar` |  |  | PAR_REPAIRABLE |
| `par_created` | `timestamp` | `timestamp_ntz` |  |  | PAR_CREATED |
| `par_createdby` | `string` | `varchar` |  |  | PAR_CREATEDBY |
| `par_updated` | `timestamp` | `timestamp_ntz` |  |  | PAR_UPDATED |
| `par_updatedby` | `string` | `varchar` |  |  | PAR_UPDATEDBY |
| `par_updatecount` | `float` | `float` |  |  | PAR_UPDATECOUNT |
| `par_savehistory` | `string` | `varchar` |  |  | PAR_SAVEHISTORY |
| `par_calstandard` | `string` | `varchar` |  |  | PAR_CALSTANDARD |
| `par_preventreorders` | `string` | `varchar` |  |  | PAR_PREVENTREORDERS |
| `par_fugitiveemissiongas` | `string` | `varchar` |  |  | PAR_FUGITIVEEMISSIONGAS |
| `par_syslevel` | `string` | `varchar` |  |  | PAR_SYSLEVEL |
| `par_asmlevel` | `string` | `varchar` |  |  | PAR_ASMLEVEL |
| `par_complevel` | `string` | `varchar` |  |  | PAR_COMPLEVEL |
| `par_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | PAR_LASTSTATUSUPDATE |
| `par_udfchar01` | `string` | `varchar` |  |  | PAR_UDFCHAR01 |
| `par_udfchar02` | `string` | `varchar` |  |  | PAR_UDFCHAR02 |
| `par_udfchar03` | `string` | `varchar` |  |  | PAR_UDFCHAR03 |
| `par_udfchar04` | `string` | `varchar` |  |  | PAR_UDFCHAR04 |
| `par_udfchar05` | `string` | `varchar` |  |  | PAR_UDFCHAR05 |
| `par_udfchar06` | `string` | `varchar` |  |  | PAR_UDFCHAR06 |
| `par_udfchar07` | `string` | `varchar` |  |  | PAR_UDFCHAR07 |
| `par_udfchar08` | `string` | `varchar` |  |  | PAR_UDFCHAR08 |
| `par_udfchar09` | `string` | `varchar` |  |  | PAR_UDFCHAR09 |
| `par_udfchar10` | `string` | `varchar` |  |  | PAR_UDFCHAR10 |
| `par_udfchar11` | `string` | `varchar` |  |  | PAR_UDFCHAR11 |
| `par_udfchar12` | `string` | `varchar` |  |  | PAR_UDFCHAR12 |
| `par_udfchar13` | `string` | `varchar` |  |  | PAR_UDFCHAR13 |
| `par_udfchar14` | `string` | `varchar` |  |  | PAR_UDFCHAR14 |
| `par_udfchar15` | `string` | `varchar` |  |  | PAR_UDFCHAR15 |
| `par_udfchar16` | `string` | `varchar` |  |  | PAR_UDFCHAR16 |
| `par_udfchar17` | `string` | `varchar` |  |  | PAR_UDFCHAR17 |
| `par_udfchar18` | `string` | `varchar` |  |  | PAR_UDFCHAR18 |
| `par_udfchar19` | `string` | `varchar` |  |  | PAR_UDFCHAR19 |
| `par_udfchar20` | `string` | `varchar` |  |  | PAR_UDFCHAR20 |
| `par_udfchar21` | `string` | `varchar` |  |  | PAR_UDFCHAR21 |
| `par_udfchar22` | `string` | `varchar` |  |  | PAR_UDFCHAR22 |
| `par_udfchar23` | `string` | `varchar` |  |  | PAR_UDFCHAR23 |
| `par_udfchar24` | `string` | `varchar` |  |  | PAR_UDFCHAR24 |
| `par_udfchar25` | `string` | `varchar` |  |  | PAR_UDFCHAR25 |
| `par_udfchar26` | `string` | `varchar` |  |  | PAR_UDFCHAR26 |
| `par_udfchar27` | `string` | `varchar` |  |  | PAR_UDFCHAR27 |
| `par_udfchar28` | `string` | `varchar` |  |  | PAR_UDFCHAR28 |
| `par_udfchar29` | `string` | `varchar` |  |  | PAR_UDFCHAR29 |
| `par_udfchar30` | `string` | `varchar` |  |  | PAR_UDFCHAR30 |
| `par_udfnum01` | `float` | `float` |  |  | PAR_UDFNUM01 |
| `par_udfnum02` | `float` | `float` |  |  | PAR_UDFNUM02 |
| `par_udfnum03` | `float` | `float` |  |  | PAR_UDFNUM03 |
| `par_udfnum04` | `float` | `float` |  |  | PAR_UDFNUM04 |
| `par_udfnum05` | `float` | `float` |  |  | PAR_UDFNUM05 |
| `par_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PAR_UDFDATE01 |
| `par_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PAR_UDFDATE02 |
| `par_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PAR_UDFDATE03 |
| `par_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PAR_UDFDATE04 |
| `par_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PAR_UDFDATE05 |
| `par_udfchkbox01` | `string` | `varchar` |  |  | PAR_UDFCHKBOX01 |
| `par_udfchkbox02` | `string` | `varchar` |  |  | PAR_UDFCHKBOX02 |
| `par_udfchkbox03` | `string` | `varchar` |  |  | PAR_UDFCHKBOX03 |
| `par_udfchkbox04` | `string` | `varchar` |  |  | PAR_UDFCHKBOX04 |
| `par_udfchkbox05` | `string` | `varchar` |  |  | PAR_UDFCHKBOX05 |
| `par_kit` | `string` | `varchar` |  |  | PAR_KIT |
| `par_safetyreviewrequired` | `timestamp` | `timestamp_ntz` |  |  | PAR_SAFETYREVIEWREQUIRED |
| `par_safetyreviewed` | `timestamp` | `timestamp_ntz` |  |  | PAR_SAFETYREVIEWED |
| `par_safetyreviewedby` | `string` | `varchar` |  |  | PAR_SAFETYREVIEWEDBY |
| `par_safetyreviewedname` | `string` | `varchar` |  |  | PAR_SAFETYREVIEWEDNAME |
| `par_safetyreviewedtype` | `string` | `varchar` |  |  | PAR_SAFETYREVIEWEDTYPE |
| `par_trackbycondition` | `string` | `varchar` |  |  | PAR_TRACKBYCONDITION |
| `par_parentpart` | `string` | `varchar` |  |  | PAR_PARENTPART |
| `par_conditiontpl` | `string` | `varchar` |  |  | PAR_CONDITIONTPL |
| `par_conditiontpl_org` | `string` | `varchar` |  |  | PAR_CONDITIONTPL_ORG |
| `par_condition` | `string` | `varchar` |  |  | PAR_CONDITION |
| `par_longdescription` | `string` | `varchar` |  |  | PAR_LONGDESCRIPTION |
| `par_documoto_bookid` | `string` | `varchar` |  |  | PAR_DOCUMOTO_BOOKID |
| `par_documoto_part` | `string` | `varchar` |  |  | PAR_DOCUMOTO_PART |
| `par_importance` | `string` | `varchar` |  |  | PAR_IMPORTANCE |
| `par_materialtype` | `string` | `varchar` |  |  | PAR_MATERIALTYPE |
| `par_udfnote01` | `string` | `varchar` |  |  | PAR_UDFNOTE01 |
| `par_udfnote02` | `string` | `varchar` |  |  | PAR_UDFNOTE02 |
| `par_profilepicture` | `string` | `varchar` |  |  | PAR_PROFILEPICTURE |
| `par_availableforcus` | `string` | `varchar` |  |  | PAR_AVAILABLEFORCUS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
