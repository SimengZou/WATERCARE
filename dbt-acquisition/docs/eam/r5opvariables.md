# r5opvariables

eam_R5OPVARIABLES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5opvariables` |
| **Materialization** | `incremental` |
| **Column count** | 90 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ovd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OVD_LASTSAVED |
| `ovd_id` | `float` | `float` |  |  | OVD_ID |
| `ovd_audituser` | `string` | `varchar` |  |  | OVD_AUDITUSER |
| `ovd_audittimestamp` | `timestamp` | `timestamp_ntz` |  |  | OVD_AUDITTIMESTAMP |
| `ovd_varnum` | `float` | `float` |  |  | OVD_VARNUM |
| `ovd_name` | `string` | `varchar` |  |  | OVD_NAME |
| `ovd_shortname` | `string` | `varchar` |  |  | OVD_SHORTNAME |
| `ovd_vartype` | `string` | `varchar` |  |  | OVD_VARTYPE |
| `ovd_allowsymbol` | `float` | `float` |  |  | OVD_ALLOWSYMBOL |
| `ovd_units` | `string` | `varchar` |  |  | OVD_UNITS |
| `ovd_entrymin` | `float` | `float` |  |  | OVD_ENTRYMIN |
| `ovd_entrymax` | `float` | `float` |  |  | OVD_ENTRYMAX |
| `ovd_qcucl` | `float` | `float` |  |  | OVD_QCUCL |
| `ovd_qclcl` | `float` | `float` |  |  | OVD_QCLCL |
| `ovd_qcuwl` | `float` | `float` |  |  | OVD_QCUWL |
| `ovd_qclwl` | `float` | `float` |  |  | OVD_QCLWL |
| `ovd_prithreshold` | `float` | `float` |  |  | OVD_PRITHRESHOLD |
| `ovd_prioperator` | `string` | `varchar` |  |  | OVD_PRIOPERATOR |
| `ovd_priopprt` | `string` | `varchar` |  |  | OVD_PRIOPPRT |
| `ovd_secthreshold` | `float` | `float` |  |  | OVD_SECTHRESHOLD |
| `ovd_secoperator` | `string` | `varchar` |  |  | OVD_SECOPERATOR |
| `ovd_secopprt` | `string` | `varchar` |  |  | OVD_SECOPPRT |
| `ovd_locid` | `float` | `float` |  |  | OVD_LOCID |
| `ovd_location` | `string` | `varchar` |  |  | OVD_LOCATION |
| `ovd_storetcode` | `string` | `varchar` |  |  | OVD_STORETCODE |
| `ovd_list` | `string` | `varchar` |  |  | OVD_LIST |
| `ovd_ud1` | `string` | `varchar` |  |  | OVD_UD1 |
| `ovd_ud2` | `string` | `varchar` |  |  | OVD_UD2 |
| `ovd_ud3` | `string` | `varchar` |  |  | OVD_UD3 |
| `ovd_ud4` | `string` | `varchar` |  |  | OVD_UD4 |
| `ovd_ud5` | `string` | `varchar` |  |  | OVD_UD5 |
| `ovd_ud6` | `string` | `varchar` |  |  | OVD_UD6 |
| `ovd_ud7` | `string` | `varchar` |  |  | OVD_UD7 |
| `ovd_ud8` | `string` | `varchar` |  |  | OVD_UD8 |
| `ovd_ud9` | `string` | `varchar` |  |  | OVD_UD9 |
| `ovd_description` | `string` | `varchar` |  |  | OVD_DESCRIPTION |
| `ovd_eqinfix` | `string` | `varchar` |  |  | OVD_EQINFIX |
| `ovd_eqpostfix` | `string` | `varchar` |  |  | OVD_EQPOSTFIX |
| `ovd_eqtranslate` | `string` | `varchar` |  |  | OVD_EQTRANSLATE |
| `ovd_calcflag` | `float` | `float` |  |  | OVD_CALCFLAG |
| `ovd_decplaces` | `float` | `float` |  |  | OVD_DECPLACES |
| `ovd_chkrollover` | `float` | `float` |  |  | OVD_CHKROLLOVER |
| `ovd_parameter` | `string` | `varchar` |  |  | OVD_PARAMETER |
| `ovd_cascaderule` | `float` | `float` |  |  | OVD_CASCADERULE |
| `ovd_postyle` | `string` | `varchar` |  |  | OVD_POSTYLE |
| `ovd_limit_to_list` | `float` | `float` |  |  | OVD_LIMIT_TO_LIST |
| `ovd_barcode` | `string` | `varchar` |  |  | OVD_BARCODE |
| `ovd_varscope` | `float` | `float` |  |  | OVD_VARSCOPE |
| `ovd_delflag` | `float` | `float` |  |  | OVD_DELFLAG |
| `ovd_modified` | `float` | `float` |  |  | OVD_MODIFIED |
| `ovd_ddspyid` | `float` | `float` |  |  | OVD_DDSPYID |
| `ovd_object` | `string` | `varchar` |  |  | OVD_OBJECT |
| `ovd_obj_org` | `string` | `varchar` |  |  | OVD_OBJ_ORG |
| `ovd_updatecount` | `float` | `float` |  |  | OVD_UPDATECOUNT |
| `ovd_qcmean` | `float` | `float` |  |  | OVD_QCMEAN |
| `ovd_org` | `string` | `varchar` |  |  | OVD_ORG |
| `ovd_attr0id` | `float` | `float` |  |  | OVD_ATTR0ID |
| `ovd_attr1id` | `float` | `float` |  |  | OVD_ATTR1ID |
| `ovd_attr2id` | `float` | `float` |  |  | OVD_ATTR2ID |
| `ovd_attr3id` | `float` | `float` |  |  | OVD_ATTR3ID |
| `ovd_attr4id` | `float` | `float` |  |  | OVD_ATTR4ID |
| `ovd_attr5id` | `float` | `float` |  |  | OVD_ATTR5ID |
| `ovd_attr6id` | `float` | `float` |  |  | OVD_ATTR6ID |
| `ovd_facttype` | `float` | `float` |  |  | OVD_FACTTYPE |
| `ovd_storeresult` | `float` | `float` |  |  | OVD_STORERESULT |
| `ovd_gridid` | `float` | `float` |  |  | OVD_GRIDID |
| `ovd_dataspyfieldids` | `string` | `varchar` |  |  | OVD_DATASPYFIELDIDS |
| `ovd_attr0entrymin` | `string` | `varchar` |  |  | OVD_ATTR0ENTRYMIN |
| `ovd_attr0entrymax` | `string` | `varchar` |  |  | OVD_ATTR0ENTRYMAX |
| `ovd_attr1entrymin` | `string` | `varchar` |  |  | OVD_ATTR1ENTRYMIN |
| `ovd_attr1entrymax` | `string` | `varchar` |  |  | OVD_ATTR1ENTRYMAX |
| `ovd_attr2entrymin` | `string` | `varchar` |  |  | OVD_ATTR2ENTRYMIN |
| `ovd_attr2entrymax` | `string` | `varchar` |  |  | OVD_ATTR2ENTRYMAX |
| `ovd_attr3entrymin` | `string` | `varchar` |  |  | OVD_ATTR3ENTRYMIN |
| `ovd_attr3entrymax` | `string` | `varchar` |  |  | OVD_ATTR3ENTRYMAX |
| `ovd_attr4entrymin` | `string` | `varchar` |  |  | OVD_ATTR4ENTRYMIN |
| `ovd_attr4entrymax` | `string` | `varchar` |  |  | OVD_ATTR4ENTRYMAX |
| `ovd_attr5entrymin` | `string` | `varchar` |  |  | OVD_ATTR5ENTRYMIN |
| `ovd_attr5entrymax` | `string` | `varchar` |  |  | OVD_ATTR5ENTRYMAX |
| `ovd_attr6entrymin` | `string` | `varchar` |  |  | OVD_ATTR6ENTRYMIN |
| `ovd_attr6entrymax` | `string` | `varchar` |  |  | OVD_ATTR6ENTRYMAX |
| `ovd_lastcalc` | `timestamp` | `timestamp_ntz` |  |  | OVD_LASTCALC |
| `ovd_calcorder` | `float` | `float` |  |  | OVD_CALCORDER |
| `ovd_dtresolution` | `float` | `float` |  |  | OVD_DTRESOLUTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
