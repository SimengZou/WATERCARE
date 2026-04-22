# r5functiontabs

eam_R5FUNCTIONTABS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5functiontabs` |
| **Materialization** | `incremental` |
| **Primary keys** | `ftb_function`, `ftb_tab` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ftb_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FTB_LASTSAVED |
| `ftb_product` | `string` | `varchar` |  |  | FTB_PRODUCT |
| `ftb_rentity` | `string` | `varchar` |  |  | FTB_RENTITY |
| `ftb_visible` | `string` | `varchar` |  |  | FTB_VISIBLE |
| `ftb_select` | `string` | `varchar` |  |  | FTB_SELECT |
| `ftb_update` | `string` | `varchar` |  |  | FTB_UPDATE |
| `ftb_insert` | `string` | `varchar` |  |  | FTB_INSERT |
| `ftb_delete` | `string` | `varchar` |  |  | FTB_DELETE |
| `ftb_displayft` | `string` | `varchar` |  |  | FTB_DISPLAYFT |
| `ftb_sysrequired` | `string` | `varchar` |  |  | FTB_SYSREQUIRED |
| `ftb_seq` | `float` | `float` |  |  | FTB_SEQ |
| `ftb_sqlexist` | `string` | `varchar` |  |  | FTB_SQLEXIST |
| `ftb_updatecount` | `float` | `float` |  |  | FTB_UPDATECOUNT |
| `ftb_altersave` | `string` | `varchar` |  |  | FTB_ALTERSAVE |
| `ftb_interfacecode` | `string` | `varchar` |  |  | FTB_INTERFACECODE |
| `ftb_mekey` | `string` | `varchar` |  |  | FTB_MEKEY |
| `ftb_ewsbtn` | `string` | `varchar` |  |  | FTB_EWSBTN |
| `ftb_xtype` | `string` | `varchar` |  |  | FTB_XTYPE |
| `ftb_nodesign` | `string` | `varchar` |  |  | FTB_NODESIGN |
| `ftb_designpopup` | `string` | `varchar` |  |  | FTB_DESIGNPOPUP |
| `ftb_function` | `string` | `varchar` | `PK` |  | FTB_FUNCTION |
| `ftb_tab` | `string` | `varchar` | `PK` |  | FTB_TAB |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
