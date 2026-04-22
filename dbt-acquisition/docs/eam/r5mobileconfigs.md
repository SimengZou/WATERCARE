# r5mobileconfigs

eam_R5MOBILECONFIGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobileconfigs` |
| **Materialization** | `incremental` |
| **Primary keys** | `mcf_code`, `mcf_rcode`, `mcf_config`, `mcf_desk` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mcf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MCF_LASTSAVED |
| `mcf_rcode` | `string` | `varchar` | `PK` |  | MCF_RCODE |
| `mcf_config` | `float` | `float` | `PK` |  | MCF_CONFIG |
| `mcf_desk` | `string` | `varchar` | `PK` |  | MCF_DESK |
| `mcf_comptype` | `string` | `varchar` |  |  | MCF_COMPTYPE |
| `mcf_group` | `string` | `varchar` |  |  | MCF_GROUP |
| `mcf_xmlconfig` | `string` | `varchar` |  |  | MCF_XMLCONFIG. Max length: 0 |
| `mcf_created` | `timestamp` | `timestamp_ntz` |  |  | MCF_CREATED |
| `mcf_updated` | `timestamp` | `timestamp_ntz` |  |  | MCF_UPDATED |
| `mcf_updatecount` | `float` | `float` |  |  | MCF_UPDATECOUNT |
| `mcf_code` | `string` | `varchar` | `PK` |  | MCF_CODE |
| `mcf_user` | `string` | `varchar` |  |  | MCF_USER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
