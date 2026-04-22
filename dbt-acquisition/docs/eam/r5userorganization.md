# r5userorganization

eam_R5USERORGANIZATION

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userorganization` |
| **Materialization** | `incremental` |
| **Primary keys** | `uog_user`, `uog_org`, `uog_role` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uog_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UOG_LASTSAVED |
| `uog_role` | `string` | `varchar` | `PK` |  | UOG_ROLE |
| `uog_default` | `string` | `varchar` |  |  | UOG_DEFAULT |
| `uog_common` | `string` | `varchar` |  |  | UOG_COMMON |
| `uog_group` | `string` | `varchar` |  |  | UOG_GROUP |
| `uog_reqappvlimit` | `float` | `float` |  |  | UOG_REQAPPVLIMIT |
| `uog_reqauthappvlimit` | `float` | `float` |  |  | UOG_REQAUTHAPPVLIMIT |
| `uog_pordappvlimit` | `float` | `float` |  |  | UOG_PORDAPPVLIMIT |
| `uog_pordauthappvlimit` | `float` | `float` |  |  | UOG_PORDAUTHAPPVLIMIT |
| `uog_picappvlimit` | `float` | `float` |  |  | UOG_PICAPPVLIMIT |
| `uog_invappvlimit` | `float` | `float` |  |  | UOG_INVAPPVLIMIT |
| `uog_invappvlimitnonpo` | `float` | `float` |  |  | UOG_INVAPPVLIMITNONPO |
| `uog_updatecount` | `float` | `float` |  |  | UOG_UPDATECOUNT |
| `uog_created` | `timestamp` | `timestamp_ntz` |  |  | UOG_CREATED |
| `uog_updated` | `timestamp` | `timestamp_ntz` |  |  | UOG_UPDATED |
| `uog_user` | `string` | `varchar` | `PK` |  | UOG_USER |
| `uog_org` | `string` | `varchar` | `PK` |  | UOG_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
