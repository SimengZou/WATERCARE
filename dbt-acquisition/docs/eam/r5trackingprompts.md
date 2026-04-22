# r5trackingprompts

eam_R5TRACKINGPROMPTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5trackingprompts` |
| **Materialization** | `incremental` |
| **Primary keys** | `tkp_trans`, `tkp_transseq` |
| **Column count** | 44 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tkp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TKP_LASTSAVED |
| `tkp_prompt` | `string` | `varchar` |  |  | TKP_PROMPT |
| `tkp_datartype` | `string` | `varchar` |  |  | TKP_DATARTYPE |
| `tkp_fixeddata` | `string` | `varchar` |  |  | TKP_FIXEDDATA |
| `tkp_computedata` | `string` | `varchar` |  |  | TKP_COMPUTEDATA |
| `tkp_uploadcolumn` | `string` | `varchar` |  |  | TKP_UPLOADCOLUMN |
| `tkp_interfacertype` | `string` | `varchar` |  |  | TKP_INTERFACERTYPE |
| `tkp_obtranstype` | `string` | `varchar` |  |  | TKP_OBTRANSTYPE |
| `tkp_transgroup` | `float` | `float` |  |  | TKP_TRANSGROUP |
| `tkp_promptseq` | `float` | `float` |  |  | TKP_PROMPTSEQ |
| `tkp_minsize` | `float` | `float` |  |  | TKP_MINSIZE |
| `tkp_maxsize` | `float` | `float` |  |  | TKP_MAXSIZE |
| `tkp_matchpattern` | `string` | `varchar` |  |  | TKP_MATCHPATTERN |
| `tkp_branchcondition` | `string` | `varchar` |  |  | TKP_BRANCHCONDITION |
| `tkp_branchpattern` | `string` | `varchar` |  |  | TKP_BRANCHPATTERN |
| `tkp_branchgoto` | `float` | `float` |  |  | TKP_BRANCHGOTO |
| `tkp_defaultprevdata` | `string` | `varchar` |  |  | TKP_DEFAULTPREVDATA |
| `tkp_returntoprompt` | `float` | `float` |  |  | TKP_RETURNTOPROMPT |
| `tkp_validatefile` | `string` | `varchar` |  |  | TKP_VALIDATEFILE |
| `tkp_obtransrtype` | `string` | `varchar` |  |  | TKP_OBTRANSRTYPE |
| `tkp_interfacetype` | `string` | `varchar` |  |  | TKP_INTERFACETYPE |
| `tkp_datatype` | `string` | `varchar` |  |  | TKP_DATATYPE |
| `tkp_column` | `string` | `varchar` |  |  | TKP_COLUMN |
| `tkp_rcolumn` | `string` | `varchar` |  |  | TKP_RCOLUMN |
| `tkp_validatelov` | `string` | `varchar` |  |  | TKP_VALIDATELOV |
| `tkp_archivecolumn` | `string` | `varchar` |  |  | TKP_ARCHIVECOLUMN |
| `tkp_arccolumn` | `string` | `varchar` |  |  | TKP_ARCCOLUMN |
| `tkp_arcrcolumn` | `string` | `varchar` |  |  | TKP_ARCRCOLUMN |
| `tkp_notonfileflag` | `string` | `varchar` |  |  | TKP_NOTONFILEFLAG |
| `tkp_lovoverrideflag` | `string` | `varchar` |  |  | TKP_LOVOVERRIDEFLAG |
| `tkp_sqlcode` | `string` | `varchar` |  |  | TKP_SQLCODE |
| `tkp_dateformat` | `string` | `varchar` |  |  | TKP_DATEFORMAT |
| `tkp_updatecount` | `float` | `float` |  |  | TKP_UPDATECOUNT |
| `tkp_updated` | `timestamp` | `timestamp_ntz` |  |  | TKP_UPDATED |
| `tkp_lovid` | `string` | `varchar` |  |  | TKP_LOVID |
| `tkp_ewsquerycode` | `string` | `varchar` |  |  | TKP_EWSQUERYCODE |
| `tkp_trans` | `string` | `varchar` | `PK` |  | TKP_TRANS |
| `tkp_transseq` | `float` | `float` | `PK` |  | TKP_TRANSSEQ |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
