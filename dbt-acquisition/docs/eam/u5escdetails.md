# u5escdetails

eam_U5ESCDETAILS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5escdetails` |
| **Materialization** | `incremental` |
| **Primary keys** | `esc_code` |
| **Column count** | 67 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `esc_assetlocation` | `string` | `varchar` |  |  | ESC_ASSETLOCATION |
| `esc_telephone` | `string` | `varchar` |  |  | ESC_TELEPHONE |
| `esc_persons` | `string` | `varchar` |  |  | ESC_PERSONS |
| `esc_additions` | `string` | `varchar` |  |  | ESC_ADDITIONS |
| `esc_aleration` | `string` | `varchar` |  |  | ESC_ALERATION |
| `esc_newwork` | `string` | `varchar` |  |  | ESC_NEWWORK |
| `esc_maintenance` | `string` | `varchar` |  |  | ESC_MAINTENANCE |
| `esc_repairs` | `string` | `varchar` |  |  | ESC_REPAIRS |
| `esc_anyone` | `string` | `varchar` |  |  | ESC_ANYONE |
| `esc_who` | `string` | `varchar` |  |  | ESC_WHO |
| `esc_mains` | `string` | `varchar` |  |  | ESC_MAINS |
| `esc_switch` | `string` | `varchar` |  |  | ESC_SWITCH |
| `esc_earth` | `string` | `varchar` |  |  | ESC_EARTH |
| `esc_lines` | `string` | `varchar` |  |  | ESC_LINES |
| `esc_desc` | `string` | `varchar` |  |  | ESC_DESC. Max length: 0 |
| `esc_iamitem` | `string` | `varchar` |  |  | ESC_IAMITEM |
| `esc_pump` | `string` | `varchar` |  |  | ESC_PUMP |
| `esc_valve` | `string` | `varchar` |  |  | ESC_VALVE |
| `esc_instruments` | `string` | `varchar` |  |  | ESC_INSTRUMENTS |
| `esc_generator` | `string` | `varchar` |  |  | ESC_GENERATOR |
| `esc_sboard` | `string` | `varchar` |  |  | ESC_SBOARD |
| `esc_pfactor` | `string` | `varchar` |  |  | ESC_PFACTOR |
| `esc_mstarter` | `string` | `varchar` |  |  | ESC_MSTARTER |
| `esc_loutlet` | `string` | `varchar` |  |  | ESC_LOUTLET |
| `esc_range` | `string` | `varchar` |  |  | ESC_RANGE |
| `esc_motor` | `string` | `varchar` |  |  | ESC_MOTOR |
| `esc_fans` | `string` | `varchar` |  |  | ESC_FANS |
| `esc_plc` | `string` | `varchar` |  |  | ESC_PLC |
| `esc_isolators` | `string` | `varchar` |  |  | ESC_ISOLATORS |
| `esc_panels` | `string` | `varchar` |  |  | ESC_PANELS |
| `esc_ups` | `string` | `varchar` |  |  | ESC_UPS |
| `esc_vsd` | `string` | `varchar` |  |  | ESC_VSD |
| `esc_soutlets` | `string` | `varchar` |  |  | ESC_SOUTLETS |
| `esc_wheaters` | `string` | `varchar` |  |  | ESC_WHEATERS |
| `esc_others` | `string` | `varchar` |  |  | ESC_OTHERS |
| `esc_comp1` | `string` | `varchar` |  |  | ESC_COMP1 |
| `esc_comp2` | `string` | `varchar` |  |  | ESC_COMP2 |
| `esc_reg58` | `string` | `varchar` |  |  | ESC_REG58 |
| `esc_earthrate` | `string` | `varchar` |  |  | ESC_EARTHRATE |
| `esc_fittings` | `string` | `varchar` |  |  | ESC_FITTINGS |
| `esc_suppliers` | `string` | `varchar` |  |  | ESC_SUPPLIERS |
| `esc_manufacturer` | `string` | `varchar` |  |  | ESC_MANUFACTURER |
| `esc_reglatns` | `string` | `varchar` |  |  | ESC_REGLATNS |
| `esc_safe` | `string` | `varchar` |  |  | ESC_SAFE |
| `esc_othercomp` | `string` | `varchar` |  |  | ESC_OTHERCOMP |
| `esc_polarity` | `string` | `varchar` |  |  | ESC_POLARITY |
| `esc_insulation` | `string` | `varchar` |  |  | ESC_INSULATION |
| `esc_continuity` | `string` | `varchar` |  |  | ESC_CONTINUITY |
| `esc_bonding` | `string` | `varchar` |  |  | ESC_BONDING |
| `esc_impedence` | `string` | `varchar` |  |  | ESC_IMPEDENCE |
| `esc_visual` | `string` | `varchar` |  |  | ESC_VISUAL |
| `esc_othertest` | `string` | `varchar` |  |  | ESC_OTHERTEST |
| `esc_eleref` | `string` | `varchar` |  |  | ESC_ELEREF |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `esc_code` | `string` | `varchar` | `PK` | `unique` | ESC_CODE |
| `esc_pew` | `string` | `varchar` |  |  | ESC_PEW |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
