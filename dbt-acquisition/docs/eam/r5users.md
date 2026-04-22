# r5users

eam_R5USERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5users` |
| **Materialization** | `incremental` |
| **Primary keys** | `usr_code` |
| **Column count** | 142 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `usr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | USR_LASTSAVED |
| `usr_code` | `string` | `varchar` | `PK` | `unique` | USR_CODE |
| `usr_desc` | `string` | `varchar` |  |  | USR_DESC |
| `usr_class` | `string` | `varchar` |  |  | USR_CLASS |
| `usr_mrc` | `string` | `varchar` |  |  | USR_MRC |
| `usr_sysman` | `string` | `varchar` |  |  | USR_SYSMAN |
| `usr_password` | `string` | `varchar` |  |  | USR_PASSWORD |
| `usr_lastfunc` | `string` | `varchar` |  |  | USR_LASTFUNC |
| `usr_firstfunc` | `string` | `varchar` |  |  | USR_FIRSTFUNC |
| `usr_group` | `string` | `varchar` |  |  | USR_GROUP |
| `usr_lang` | `string` | `varchar` |  |  | USR_LANG |
| `usr_printer` | `string` | `varchar` |  |  | USR_PRINTER |
| `usr_exppass` | `timestamp` | `timestamp_ntz` |  |  | USR_EXPPASS |
| `usr_expuser` | `timestamp` | `timestamp_ntz` |  |  | USR_EXPUSER |
| `usr_facility` | `string` | `varchar` |  |  | USR_FACILITY |
| `usr_reqappvlimit` | `float` | `float` |  |  | USR_REQAPPVLIMIT |
| `usr_reqauthappvlimit` | `float` | `float` |  |  | USR_REQAUTHAPPVLIMIT |
| `usr_buyer` | `string` | `varchar` |  |  | USR_BUYER |
| `usr_pordappvlimit` | `float` | `float` |  |  | USR_PORDAPPVLIMIT |
| `usr_pordauthappvlimit` | `float` | `float` |  |  | USR_PORDAUTHAPPVLIMIT |
| `usr_router` | `string` | `varchar` |  |  | USR_ROUTER |
| `usr_datelocked` | `timestamp` | `timestamp_ntz` |  |  | USR_DATELOCKED |
| `usr_violations` | `float` | `float` |  |  | USR_VIOLATIONS |
| `usr_class_org` | `string` | `varchar` |  |  | USR_CLASS_ORG |
| `usr_approver` | `string` | `varchar` |  |  | USR_APPROVER |
| `usr_picappvlimit` | `float` | `float` |  |  | USR_PICAPPVLIMIT |
| `usr_emailaddress` | `string` | `varchar` |  |  | USR_EMAILADDRESS |
| `usr_notificationpref` | `string` | `varchar` |  |  | USR_NOTIFICATIONPREF |
| `usr_browser` | `string` | `varchar` |  |  | USR_BROWSER |
| `usr_invappvlimit` | `float` | `float` |  |  | USR_INVAPPVLIMIT |
| `usr_invappvlimitnonpo` | `float` | `float` |  |  | USR_INVAPPVLIMITNONPO |
| `usr_updatecount` | `float` | `float` |  |  | USR_UPDATECOUNT |
| `usr_login` | `string` | `varchar` |  |  | USR_LOGIN |
| `usr_requestor` | `string` | `varchar` |  |  | USR_REQUESTOR |
| `usr_active` | `string` | `varchar` |  |  | USR_ACTIVE |
| `usr_locale` | `string` | `varchar` |  |  | USR_LOCALE |
| `usr_successmsgtimeout` | `float` | `float` |  |  | USR_SUCCESSMSGTIMEOUT |
| `usr_ewsfirstfunc` | `string` | `varchar` |  |  | USR_EWSFIRSTFUNC |
| `usr_created` | `timestamp` | `timestamp_ntz` |  |  | USR_CREATED |
| `usr_updated` | `timestamp` | `timestamp_ntz` |  |  | USR_UPDATED |
| `usr_inboxtab` | `string` | `varchar` |  |  | USR_INBOXTAB |
| `usr_mobile` | `string` | `varchar` |  |  | USR_MOBILE |
| `usr_connector` | `string` | `varchar` |  |  | USR_CONNECTOR |
| `usr_barcode` | `string` | `varchar` |  |  | USR_BARCODE |
| `usr_notused` | `string` | `varchar` |  |  | USR_NOTUSED |
| `usr_sessiontimeout` | `float` | `float` |  |  | USR_SESSIONTIMEOUT |
| `usr_analytics` | `string` | `varchar` |  |  | USR_ANALYTICS |
| `usr_professional` | `string` | `varchar` |  |  | USR_PROFESSIONAL |
| `usr_consumer` | `string` | `varchar` |  |  | USR_CONSUMER |
| `usr_screendesigner` | `string` | `varchar` |  |  | USR_SCREENDESIGNER |
| `usr_508` | `string` | `varchar` |  |  | USR_508 |
| `usr_mobileadm` | `string` | `varchar` |  |  | USR_MOBILEADM |
| `usr_editdataspy` | `string` | `varchar` |  |  | USR_EDITDATASPY |
| `usr_advancedfilters` | `string` | `varchar` |  |  | USR_ADVANCEDFILTERS |
| `usr_editowncomments` | `string` | `varchar` |  |  | USR_EDITOWNCOMMENTS |
| `usr_editotherscomments` | `string` | `varchar` |  |  | USR_EDITOTHERSCOMMENTS |
| `usr_udfchar01` | `string` | `varchar` |  |  | USR_UDFCHAR01 |
| `usr_udfchar02` | `string` | `varchar` |  |  | USR_UDFCHAR02 |
| `usr_udfchar03` | `string` | `varchar` |  |  | USR_UDFCHAR03 |
| `usr_udfchar04` | `string` | `varchar` |  |  | USR_UDFCHAR04 |
| `usr_udfchar05` | `string` | `varchar` |  |  | USR_UDFCHAR05 |
| `usr_udfchar06` | `string` | `varchar` |  |  | USR_UDFCHAR06 |
| `usr_udfchar07` | `string` | `varchar` |  |  | USR_UDFCHAR07 |
| `usr_udfchar08` | `string` | `varchar` |  |  | USR_UDFCHAR08 |
| `usr_udfchar09` | `string` | `varchar` |  |  | USR_UDFCHAR09 |
| `usr_udfchar10` | `string` | `varchar` |  |  | USR_UDFCHAR10 |
| `usr_udfchar11` | `string` | `varchar` |  |  | USR_UDFCHAR11 |
| `usr_udfchar12` | `string` | `varchar` |  |  | USR_UDFCHAR12 |
| `usr_udfchar13` | `string` | `varchar` |  |  | USR_UDFCHAR13 |
| `usr_udfchar14` | `string` | `varchar` |  |  | USR_UDFCHAR14 |
| `usr_udfchar15` | `string` | `varchar` |  |  | USR_UDFCHAR15 |
| `usr_udfchar16` | `string` | `varchar` |  |  | USR_UDFCHAR16 |
| `usr_udfchar17` | `string` | `varchar` |  |  | USR_UDFCHAR17 |
| `usr_udfchar18` | `string` | `varchar` |  |  | USR_UDFCHAR18 |
| `usr_udfchar19` | `string` | `varchar` |  |  | USR_UDFCHAR19 |
| `usr_udfchar20` | `string` | `varchar` |  |  | USR_UDFCHAR20 |
| `usr_udfchar21` | `string` | `varchar` |  |  | USR_UDFCHAR21 |
| `usr_udfchar22` | `string` | `varchar` |  |  | USR_UDFCHAR22 |
| `usr_udfchar23` | `string` | `varchar` |  |  | USR_UDFCHAR23 |
| `usr_udfchar24` | `string` | `varchar` |  |  | USR_UDFCHAR24 |
| `usr_udfchar25` | `string` | `varchar` |  |  | USR_UDFCHAR25 |
| `usr_udfchar26` | `string` | `varchar` |  |  | USR_UDFCHAR26 |
| `usr_udfchar27` | `string` | `varchar` |  |  | USR_UDFCHAR27 |
| `usr_udfchar28` | `string` | `varchar` |  |  | USR_UDFCHAR28 |
| `usr_udfchar29` | `string` | `varchar` |  |  | USR_UDFCHAR29 |
| `usr_udfchar30` | `string` | `varchar` |  |  | USR_UDFCHAR30 |
| `usr_udfnum01` | `float` | `float` |  |  | USR_UDFNUM01 |
| `usr_udfnum02` | `float` | `float` |  |  | USR_UDFNUM02 |
| `usr_udfnum03` | `float` | `float` |  |  | USR_UDFNUM03 |
| `usr_udfnum04` | `float` | `float` |  |  | USR_UDFNUM04 |
| `usr_udfnum05` | `float` | `float` |  |  | USR_UDFNUM05 |
| `usr_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | USR_UDFDATE01 |
| `usr_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | USR_UDFDATE02 |
| `usr_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | USR_UDFDATE03 |
| `usr_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | USR_UDFDATE04 |
| `usr_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | USR_UDFDATE05 |
| `usr_udfchkbox01` | `string` | `varchar` |  |  | USR_UDFCHKBOX01 |
| `usr_udfchkbox02` | `string` | `varchar` |  |  | USR_UDFCHKBOX02 |
| `usr_udfchkbox03` | `string` | `varchar` |  |  | USR_UDFCHKBOX03 |
| `usr_udfchkbox04` | `string` | `varchar` |  |  | USR_UDFCHKBOX04 |
| `usr_udfchkbox05` | `string` | `varchar` |  |  | USR_UDFCHKBOX05 |
| `usr_enxuser` | `string` | `varchar` |  |  | USR_ENXUSER |
| `usr_globaldataspy` | `string` | `varchar` |  |  | USR_GLOBALDATASPY |
| `usr_default_org` | `string` | `varchar` |  |  | USR_DEFAULT_ORG |
| `usr_default_chart_type` | `string` | `varchar` |  |  | USR_DEFAULT_CHART_TYPE |
| `usr_default_chart_period` | `string` | `varchar` |  |  | USR_DEFAULT_CHART_PERIOD |
| `usr_default_chart_groupby` | `string` | `varchar` |  |  | USR_DEFAULT_CHART_GROUPBY |
| `usr_default_chart_showyear` | `string` | `varchar` |  |  | USR_DEFAULT_CHART_SHOWYEAR |
| `usr_default_kpi_size` | `float` | `float` |  |  | USR_DEFAULT_KPI_SIZE |
| `usr_externcode` | `string` | `varchar` |  |  | USR_EXTERNCODE |
| `usr_canceldailyschedsession` | `string` | `varchar` |  |  | USR_CANCELDAILYSCHEDSESSION |
| `usr_supplier` | `string` | `varchar` |  |  | USR_SUPPLIER |
| `usr_supplier_org` | `string` | `varchar` |  |  | USR_SUPPLIER_ORG |
| `usr_allowscreencachesetup` | `string` | `varchar` |  |  | USR_ALLOWSCREENCACHESETUP |
| `usr_allowcreateimportutiltemp` | `string` | `varchar` |  |  | USR_ALLOWCREATEIMPORTUTILTEMP |
| `usr_allowviewingaudittrail` | `string` | `varchar` |  |  | USR_ALLOWVIEWINGAUDITTRAIL |
| `usr_enablescreencachedeck` | `string` | `varchar` |  |  | USR_ENABLESCREENCACHEDECK |
| `usr_enabletransitionanimations` | `string` | `varchar` |  |  | USR_ENABLETRANSITIONANIMATIONS |
| `usr_default_chart_state` | `string` | `varchar` |  |  | USR_DEFAULT_CHART_STATE |
| `usr_defaultstore` | `string` | `varchar` |  |  | USR_DEFAULTSTORE |
| `usr_allowmobilesettings` | `string` | `varchar` |  |  | USR_ALLOWMOBILESETTINGS |
| `usr_arcgis_usercode` | `string` | `varchar` |  |  | USR_ARCGIS_USERCODE |
| `usr_arcgis_password` | `string` | `varchar` |  |  | USR_ARCGIS_PASSWORD |
| `usr_allowviewingprivatecases` | `string` | `varchar` |  |  | USR_ALLOWVIEWINGPRIVATECASES |
| `usr_mobilefirstscreen` | `string` | `varchar` |  |  | USR_MOBILEFIRSTSCREEN |
| `usr_allowcontainssearch` | `string` | `varchar` |  |  | USR_ALLOWCONTAINSSEARCH |
| `usr_allowupdatefield` | `string` | `varchar` |  |  | USR_ALLOWUPDATEFIELD |
| `usr_profilepicture` | `string` | `varchar` |  |  | USR_PROFILEPICTURE |
| `usr_mobileautosync` | `float` | `float` |  |  | USR_MOBILEAUTOSYNC |
| `usr_enablebiometrics` | `string` | `varchar` |  |  | USR_ENABLEBIOMETRICS |
| `usr_duxfirstfunc` | `string` | `varchar` |  |  | USR_DUXFIRSTFUNC |
| `usr_mobileapp` | `string` | `varchar` |  |  | USR_MOBILEAPP |
| `usr_allowmobilesyncconfig` | `string` | `varchar` |  |  | USR_ALLOWMOBILESYNCCONFIG |
| `usr_enablemobilerestoreuser` | `string` | `varchar` |  |  | USR_ENABLEMOBILERESTOREUSER |
| `usr_pwdresetlinkgentime` | `timestamp` | `timestamp_ntz` |  |  | USR_PWDRESETLINKGENTIME |
| `usr_pwdresetcount` | `float` | `float` |  |  | USR_PWDRESETCOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
