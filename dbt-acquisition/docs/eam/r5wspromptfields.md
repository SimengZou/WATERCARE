# r5wspromptfields

eam_R5WSPROMPTFIELDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wspromptfields` |
| **Materialization** | `incremental` |
| **Primary keys** | `wsf_wspmtcode`, `wsf_line` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wsf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WSF_LASTSAVED |
| `wsf_responsexpath` | `string` | `varchar` |  |  | WSF_RESPONSEXPATH |
| `wsf_line` | `float` | `float` | `PK` |  | WSF_LINE |
| `wsf_field` | `string` | `varchar` |  |  | WSF_FIELD |
| `wsf_fieldlabel` | `string` | `varchar` |  |  | WSF_FIELDLABEL |
| `wsf_nextline` | `float` | `float` |  |  | WSF_NEXTLINE |
| `wsf_rtype` | `string` | `varchar` |  |  | WSF_RTYPE |
| `wsf_type` | `string` | `varchar` |  |  | WSF_TYPE |
| `wsf_maxlength` | `float` | `float` |  |  | WSF_MAXLENGTH |
| `wsf_minlength` | `float` | `float` |  |  | WSF_MINLENGTH |
| `wsf_dupprevvalue` | `string` | `varchar` |  |  | WSF_DUPPREVVALUE |
| `wsf_branchcond` | `string` | `varchar` |  |  | WSF_BRANCHCOND |
| `wsf_branchpattern` | `string` | `varchar` |  |  | WSF_BRANCHPATTERN |
| `wsf_branchgoto` | `float` | `float` |  |  | WSF_BRANCHGOTO |
| `wsf_retrievefromgroup` | `float` | `float` |  |  | WSF_RETRIEVEFROMGROUP |
| `wsf_retrievefield` | `string` | `varchar` |  |  | WSF_RETRIEVEFIELD |
| `wsf_destwebservice` | `string` | `varchar` |  |  | WSF_DESTWEBSERVICE |
| `wsf_retrievexpath` | `string` | `varchar` |  |  | WSF_RETRIEVEXPATH |
| `wsf_destxpath` | `string` | `varchar` |  |  | WSF_DESTXPATH |
| `wsf_fieldxpath` | `string` | `varchar` |  |  | WSF_FIELDXPATH |
| `wsf_fixeddata` | `string` | `varchar` |  |  | WSF_FIXEDDATA |
| `wsf_computeddata` | `string` | `varchar` |  |  | WSF_COMPUTEDDATA |
| `wsf_pattern` | `string` | `varchar` |  |  | WSF_PATTERN |
| `wsf_sqlquerycode` | `string` | `varchar` |  |  | WSF_SQLQUERYCODE |
| `wsf_displaytype` | `string` | `varchar` |  |  | WSF_DISPLAYTYPE |
| `wsf_unmapped` | `string` | `varchar` |  |  | WSF_UNMAPPED |
| `wsf_updatecount` | `float` | `float` |  |  | WSF_UPDATECOUNT |
| `wsf_updated` | `timestamp` | `timestamp_ntz` |  |  | WSF_UPDATED |
| `wsf_mobilequerycode` | `string` | `varchar` |  |  | WSF_MOBILEQUERYCODE |
| `wsf_iscontrolorg` | `string` | `varchar` |  |  | WSF_ISCONTROLORG |
| `wsf_isclass` | `string` | `varchar` |  |  | WSF_ISCLASS |
| `wsf_isclassorg` | `string` | `varchar` |  |  | WSF_ISCLASSORG |
| `wsf_iscategory` | `string` | `varchar` |  |  | WSF_ISCATEGORY |
| `wsf_systemfieldtype` | `string` | `varchar` |  |  | WSF_SYSTEMFIELDTYPE |
| `wsf_customfieldid` | `string` | `varchar` |  |  | WSF_CUSTOMFIELDID |
| `wsf_clearprevvalues` | `string` | `varchar` |  |  | WSF_CLEARPREVVALUES |
| `wsf_tagname` | `string` | `varchar` |  |  | WSF_TAGNAME |
| `wsf_primarykey` | `string` | `varchar` |  |  | WSF_PRIMARYKEY |
| `wsf_precision` | `float` | `float` |  |  | WSF_PRECISION |
| `wsf_scale` | `float` | `float` |  |  | WSF_SCALE |
| `wsf_uppercase` | `string` | `varchar` |  |  | WSF_UPPERCASE |
| `wsf_wspmtcode` | `string` | `varchar` | `PK` |  | WSF_WSPMTCODE |
| `wsf_processgroup` | `float` | `float` |  |  | WSF_PROCESSGROUP |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
