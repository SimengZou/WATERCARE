# r5dddataspy

eam_R5DDDATASPY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dddataspy` |
| **Materialization** | `incremental` |
| **Primary keys** | `dds_ddspyid` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dds_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DDS_LASTSAVED |
| `dds_autorun` | `string` | `varchar` |  |  | DDS_AUTORUN |
| `dds_scope` | `string` | `varchar` |  |  | DDS_SCOPE |
| `dds_organization` | `string` | `varchar` |  |  | DDS_ORGANIZATION |
| `dds_updatestamp` | `timestamp` | `timestamp_ntz` |  |  | DDS_UPDATESTAMP |
| `dds_updatebyuser` | `string` | `varchar` |  |  | DDS_UPDATEBYUSER |
| `dds_createdstamp` | `timestamp` | `timestamp_ntz` |  |  | DDS_CREATEDSTAMP |
| `dds_updateable` | `string` | `varchar` |  |  | DDS_UPDATEABLE |
| `dds_filterstrxml` | `string` | `varchar` |  |  | DDS_FILTERSTRXML |
| `dds_sortstrxml` | `string` | `varchar` |  |  | DDS_SORTSTRXML |
| `dds_fieldlist` | `string` | `varchar` |  |  | DDS_FIELDLIST |
| `dds_updatecount` | `float` | `float` |  |  | DDS_UPDATECOUNT |
| `dds_displayrow` | `float` | `float` |  |  | DDS_DISPLAYROW |
| `dds_owner` | `string` | `varchar` |  |  | DDS_OWNER |
| `dds_gridid` | `float` | `float` |  |  | DDS_GRIDID |
| `dds_type` | `string` | `varchar` |  |  | DDS_TYPE |
| `dds_defaultflag` | `string` | `varchar` |  |  | DDS_DEFAULTFLAG |
| `dds_fieldlist_portlet` | `string` | `varchar` |  |  | DDS_FIELDLIST_PORTLET |
| `dds_clientrows` | `float` | `float` |  |  | DDS_CLIENTROWS |
| `dds_portletrows` | `float` | `float` |  |  | DDS_PORTLETROWS |
| `dds_hints` | `string` | `varchar` |  |  | DDS_HINTS |
| `dds_botname` | `string` | `varchar` |  |  | DDS_BOTNAME |
| `dds_userfilter` | `string` | `varchar` |  |  | DDS_USERFILTER |
| `dds_securitydataspy` | `string` | `varchar` |  |  | DDS_SECURITYDATASPY |
| `dds_mekey` | `string` | `varchar` |  |  | DDS_MEKEY |
| `dds_updated` | `timestamp` | `timestamp_ntz` |  |  | DDS_UPDATED |
| `dds_ddspyfilter` | `string` | `varchar` |  |  | DDS_DDSPYFILTER |
| `dds_globaldataspy` | `string` | `varchar` |  |  | DDS_GLOBALDATASPY |
| `dds_blacklistviolations` | `float` | `float` |  |  | DDS_BLACKLISTVIOLATIONS |
| `dds_ddspyname` | `string` | `varchar` |  |  | DDS_DDSPYNAME |
| `dds_ddspyid` | `float` | `float` | `PK` | `unique` | DDS_DDSPYID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
