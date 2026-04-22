# r5reportfilters

eam_R5REPORTFILTERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reportfilters` |
| **Materialization** | `incremental` |
| **Primary keys** | `rfi_user`, `rfi_func`, `rfi_name` |
| **Column count** | 86 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rfi_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RFI_LASTSAVED |
| `rfi_user` | `string` | `varchar` | `PK` |  | RFI_USER |
| `rfi_func` | `string` | `varchar` | `PK` |  | RFI_FUNC |
| `rfi_name` | `string` | `varchar` | `PK` |  | RFI_NAME |
| `rfi_default` | `string` | `varchar` |  |  | RFI_DEFAULT |
| `rfi_param1` | `string` | `varchar` |  |  | RFI_PARAM1 |
| `rfi_param2` | `string` | `varchar` |  |  | RFI_PARAM2 |
| `rfi_param3` | `string` | `varchar` |  |  | RFI_PARAM3 |
| `rfi_param4` | `string` | `varchar` |  |  | RFI_PARAM4 |
| `rfi_param5` | `string` | `varchar` |  |  | RFI_PARAM5 |
| `rfi_param6` | `string` | `varchar` |  |  | RFI_PARAM6 |
| `rfi_param7` | `string` | `varchar` |  |  | RFI_PARAM7 |
| `rfi_param8` | `string` | `varchar` |  |  | RFI_PARAM8 |
| `rfi_param9` | `string` | `varchar` |  |  | RFI_PARAM9 |
| `rfi_param10` | `string` | `varchar` |  |  | RFI_PARAM10 |
| `rfi_param11` | `string` | `varchar` |  |  | RFI_PARAM11 |
| `rfi_param12` | `string` | `varchar` |  |  | RFI_PARAM12 |
| `rfi_param13` | `string` | `varchar` |  |  | RFI_PARAM13 |
| `rfi_param14` | `string` | `varchar` |  |  | RFI_PARAM14 |
| `rfi_param15` | `string` | `varchar` |  |  | RFI_PARAM15 |
| `rfi_param16` | `string` | `varchar` |  |  | RFI_PARAM16 |
| `rfi_param17` | `string` | `varchar` |  |  | RFI_PARAM17 |
| `rfi_param18` | `string` | `varchar` |  |  | RFI_PARAM18 |
| `rfi_param19` | `string` | `varchar` |  |  | RFI_PARAM19 |
| `rfi_param20` | `string` | `varchar` |  |  | RFI_PARAM20 |
| `rfi_param21` | `string` | `varchar` |  |  | RFI_PARAM21 |
| `rfi_param22` | `string` | `varchar` |  |  | RFI_PARAM22 |
| `rfi_param23` | `string` | `varchar` |  |  | RFI_PARAM23 |
| `rfi_param24` | `string` | `varchar` |  |  | RFI_PARAM24 |
| `rfi_param25` | `string` | `varchar` |  |  | RFI_PARAM25 |
| `rfi_param26` | `string` | `varchar` |  |  | RFI_PARAM26 |
| `rfi_param27` | `string` | `varchar` |  |  | RFI_PARAM27 |
| `rfi_param28` | `string` | `varchar` |  |  | RFI_PARAM28 |
| `rfi_param29` | `string` | `varchar` |  |  | RFI_PARAM29 |
| `rfi_param30` | `string` | `varchar` |  |  | RFI_PARAM30 |
| `rfi_chkbox1` | `string` | `varchar` |  |  | RFI_CHKBOX1 |
| `rfi_chkbox2` | `string` | `varchar` |  |  | RFI_CHKBOX2 |
| `rfi_chkbox3` | `string` | `varchar` |  |  | RFI_CHKBOX3 |
| `rfi_chkbox4` | `string` | `varchar` |  |  | RFI_CHKBOX4 |
| `rfi_chkbox5` | `string` | `varchar` |  |  | RFI_CHKBOX5 |
| `rfi_chkbox6` | `string` | `varchar` |  |  | RFI_CHKBOX6 |
| `rfi_radio` | `string` | `varchar` |  |  | RFI_RADIO |
| `rfi_org` | `string` | `varchar` |  |  | RFI_ORG |
| `rfi_fromdate` | `timestamp` | `timestamp_ntz` |  |  | RFI_FROMDATE |
| `rfi_todate` | `timestamp` | `timestamp_ntz` |  |  | RFI_TODATE |
| `rfi_chkbox7` | `string` | `varchar` |  |  | RFI_CHKBOX7 |
| `rfi_updatecount` | `float` | `float` |  |  | RFI_UPDATECOUNT |
| `rfi_updated` | `timestamp` | `timestamp_ntz` |  |  | RFI_UPDATED |
| `rfi_visflds` | `string` | `varchar` |  |  | RFI_VISFLDS |
| `rfi_orderby` | `string` | `varchar` |  |  | RFI_ORDERBY |
| `rfi_ordertype` | `string` | `varchar` |  |  | RFI_ORDERTYPE |
| `rfi_groupby` | `string` | `varchar` |  |  | RFI_GROUPBY |
| `rfi_chkbox8` | `string` | `varchar` |  |  | RFI_CHKBOX8 |
| `rfi_chkbox9` | `string` | `varchar` |  |  | RFI_CHKBOX9 |
| `rfi_chkbox10` | `string` | `varchar` |  |  | RFI_CHKBOX10 |
| `rfi_chkbox11` | `string` | `varchar` |  |  | RFI_CHKBOX11 |
| `rfi_chkbox12` | `string` | `varchar` |  |  | RFI_CHKBOX12 |
| `rfi_chkbox13` | `string` | `varchar` |  |  | RFI_CHKBOX13 |
| `rfi_chkbox14` | `string` | `varchar` |  |  | RFI_CHKBOX14 |
| `rfi_chkbox15` | `string` | `varchar` |  |  | RFI_CHKBOX15 |
| `rfi_param31` | `string` | `varchar` |  |  | RFI_PARAM31 |
| `rfi_param32` | `string` | `varchar` |  |  | RFI_PARAM32 |
| `rfi_param33` | `string` | `varchar` |  |  | RFI_PARAM33 |
| `rfi_param34` | `string` | `varchar` |  |  | RFI_PARAM34 |
| `rfi_param35` | `string` | `varchar` |  |  | RFI_PARAM35 |
| `rfi_param36` | `string` | `varchar` |  |  | RFI_PARAM36 |
| `rfi_param37` | `string` | `varchar` |  |  | RFI_PARAM37 |
| `rfi_param38` | `string` | `varchar` |  |  | RFI_PARAM38 |
| `rfi_param39` | `string` | `varchar` |  |  | RFI_PARAM39 |
| `rfi_param40` | `string` | `varchar` |  |  | RFI_PARAM40 |
| `rfi_chkbox16` | `string` | `varchar` |  |  | RFI_CHKBOX16 |
| `rfi_chkbox17` | `string` | `varchar` |  |  | RFI_CHKBOX17 |
| `rfi_chkbox18` | `string` | `varchar` |  |  | RFI_CHKBOX18 |
| `rfi_chkbox19` | `string` | `varchar` |  |  | RFI_CHKBOX19 |
| `rfi_chkbox20` | `string` | `varchar` |  |  | RFI_CHKBOX20 |
| `rfi_chkbox21` | `string` | `varchar` |  |  | RFI_CHKBOX21 |
| `rfi_chkbox22` | `string` | `varchar` |  |  | RFI_CHKBOX22 |
| `rfi_chkbox23` | `string` | `varchar` |  |  | RFI_CHKBOX23 |
| `rfi_chkbox24` | `string` | `varchar` |  |  | RFI_CHKBOX24 |
| `rfi_chkbox25` | `string` | `varchar` |  |  | RFI_CHKBOX25 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
