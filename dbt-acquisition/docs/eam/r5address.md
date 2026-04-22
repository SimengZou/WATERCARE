# r5address

eam_R5ADDRESS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5address` |
| **Materialization** | `incremental` |
| **Primary keys** | `adr_code`, `adr_rentity`, `adr_rtype` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `adr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ADR_LASTSAVED |
| `adr_rtype` | `string` | `varchar` | `PK` |  | ADR_RTYPE |
| `adr_text` | `string` | `varchar` |  |  | ADR_TEXT |
| `adr_address1` | `string` | `varchar` |  |  | ADR_ADDRESS1 |
| `adr_address2` | `string` | `varchar` |  |  | ADR_ADDRESS2 |
| `adr_address3` | `string` | `varchar` |  |  | ADR_ADDRESS3 |
| `adr_city` | `string` | `varchar` |  |  | ADR_CITY |
| `adr_state` | `string` | `varchar` |  |  | ADR_STATE |
| `adr_zip` | `string` | `varchar` |  |  | ADR_ZIP |
| `adr_country` | `string` | `varchar` |  |  | ADR_COUNTRY |
| `adr_phone` | `string` | `varchar` |  |  | ADR_PHONE |
| `adr_phoneextn` | `string` | `varchar` |  |  | ADR_PHONEEXTN |
| `adr_fax` | `string` | `varchar` |  |  | ADR_FAX |
| `adr_email` | `string` | `varchar` |  |  | ADR_EMAIL |
| `adr_updatecount` | `float` | `float` |  |  | ADR_UPDATECOUNT |
| `adr_rentity` | `string` | `varchar` | `PK` |  | ADR_RENTITY |
| `adr_code` | `string` | `varchar` | `PK` |  | ADR_CODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
