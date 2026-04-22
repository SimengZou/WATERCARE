# r5udfscreenfields

eam_R5UDFSCREENFIELDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5udfscreenfields` |
| **Materialization** | `incremental` |
| **Primary keys** | `usf_screenname`, `usf_fieldname` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `usf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | USF_LASTSAVED |
| `usf_desc` | `string` | `varchar` |  |  | USF_DESC |
| `usf_sequence` | `float` | `float` |  |  | USF_SEQUENCE |
| `usf_maxlength` | `float` | `float` |  |  | USF_MAXLENGTH |
| `usf_precision` | `float` | `float` |  |  | USF_PRECISION |
| `usf_scale` | `float` | `float` |  |  | USF_SCALE |
| `usf_fieldlabel` | `string` | `varchar` |  |  | USF_FIELDLABEL |
| `usf_fieldtype` | `string` | `varchar` |  |  | USF_FIELDTYPE |
| `usf_primary` | `string` | `varchar` |  |  | USF_PRIMARY |
| `usf_notused` | `string` | `varchar` |  |  | USF_NOTUSED |
| `usf_nullable` | `string` | `varchar` |  |  | USF_NULLABLE |
| `usf_parentfield` | `string` | `varchar` |  |  | USF_PARENTFIELD |
| `usf_lookupsource` | `string` | `varchar` |  |  | USF_LOOKUPSOURCE |
| `usf_lookupquery` | `string` | `varchar` |  |  | USF_LOOKUPQUERY |
| `usf_computeddata` | `string` | `varchar` |  |  | USF_COMPUTEDDATA |
| `usf_uppercase` | `string` | `varchar` |  |  | USF_UPPERCASE |
| `usf_created` | `timestamp` | `timestamp_ntz` |  |  | USF_CREATED |
| `usf_updated` | `timestamp` | `timestamp_ntz` |  |  | USF_UPDATED |
| `usf_createdby` | `string` | `varchar` |  |  | USF_CREATEDBY |
| `usf_updatedby` | `string` | `varchar` |  |  | USF_UPDATEDBY |
| `usf_updatecount` | `float` | `float` |  |  | USF_UPDATECOUNT |
| `usf_generated` | `string` | `varchar` |  |  | USF_GENERATED |
| `usf_retrievedvaluelookup` | `string` | `varchar` |  |  | USF_RETRIEVEDVALUELOOKUP |
| `usf_droppdown` | `string` | `varchar` |  |  | USF_DROPPDOWN |
| `usf_screenname` | `string` | `varchar` | `PK` |  | USF_SCREENNAME |
| `usf_fieldname` | `string` | `varchar` | `PK` |  | USF_FIELDNAME |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
