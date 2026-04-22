# r5defaultpagelayout

eam_R5DEFAULTPAGELAYOUT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5defaultpagelayout` |
| **Materialization** | `incremental` |
| **Primary keys** | `pld_pagename`, `pld_elementid` |
| **Column count** | 36 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pld_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PLD_LASTSAVED |
| `pld_responsexpath` | `string` | `varchar` |  |  | PLD_RESPONSEXPATH |
| `pld_elementtype` | `string` | `varchar` |  |  | PLD_ELEMENTTYPE |
| `pld_presentinjsp` | `string` | `varchar` |  |  | PLD_PRESENTINJSP |
| `pld_attribute` | `string` | `varchar` |  |  | PLD_ATTRIBUTE |
| `pld_fieldcontainer` | `string` | `varchar` |  |  | PLD_FIELDCONTAINER |
| `pld_fieldgroup` | `float` | `float` |  |  | PLD_FIELDGROUP |
| `pld_positioningroup` | `float` | `float` |  |  | PLD_POSITIONINGROUP |
| `pld_fieldconttype` | `float` | `float` |  |  | PLD_FIELDCONTTYPE |
| `pld_tabindex` | `float` | `float` |  |  | PLD_TABINDEX |
| `pld_systemattribute` | `string` | `varchar` |  |  | PLD_SYSTEMATTRIBUTE |
| `pld_updatecount` | `float` | `float` |  |  | PLD_UPDATECOUNT |
| `pld_ddtablename` | `string` | `varchar` |  |  | PLD_DDTABLENAME |
| `pld_ddfieldname` | `string` | `varchar` |  |  | PLD_DDFIELDNAME |
| `pld_xpath` | `string` | `varchar` |  |  | PLD_XPATH |
| `pld_fieldtype` | `string` | `varchar` |  |  | PLD_FIELDTYPE |
| `pld_size` | `float` | `float` |  |  | PLD_SIZE |
| `pld_maxlength` | `float` | `float` |  |  | PLD_MAXLENGTH |
| `pld_case` | `string` | `varchar` |  |  | PLD_CASE |
| `pld_otherattribs` | `string` | `varchar` |  |  | PLD_OTHERATTRIBS |
| `pld_numbertype` | `string` | `varchar` |  |  | PLD_NUMBERTYPE |
| `pld_onlookup` | `string` | `varchar` |  |  | PLD_ONLOOKUP |
| `pld_onvalidate` | `string` | `varchar` |  |  | PLD_ONVALIDATE |
| `pld_onclasschange` | `string` | `varchar` |  |  | PLD_ONCLASSCHANGE |
| `pld_onchainedlookup` | `string` | `varchar` |  |  | PLD_ONCHAINEDLOOKUP |
| `pld_jspfile` | `string` | `varchar` |  |  | PLD_JSPFILE |
| `pld_othertags` | `string` | `varchar` |  |  | PLD_OTHERTAGS |
| `pld_destxpath` | `string` | `varchar` |  |  | PLD_DESTXPATH |
| `pld_pagename` | `string` | `varchar` | `PK` |  | PLD_PAGENAME |
| `pld_elementid` | `string` | `varchar` | `PK` |  | PLD_ELEMENTID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
