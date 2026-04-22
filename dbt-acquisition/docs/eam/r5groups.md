# r5groups

eam_R5GROUPS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5groups` |
| **Materialization** | `incremental` |
| **Primary keys** | `ugr_code` |
| **Column count** | 69 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ugr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UGR_LASTSAVED |
| `ugr_mobileapp` | `string` | `varchar` |  |  | UGR_MOBILEAPP |
| `ugr_class` | `string` | `varchar` |  |  | UGR_CLASS |
| `ugr_mrc` | `string` | `varchar` |  |  | UGR_MRC |
| `ugr_printer` | `string` | `varchar` |  |  | UGR_PRINTER |
| `ugr_corrbook` | `string` | `varchar` |  |  | UGR_CORRBOOK |
| `ugr_class_org` | `string` | `varchar` |  |  | UGR_CLASS_ORG |
| `ugr_login` | `string` | `varchar` |  |  | UGR_LOGIN |
| `ugr_requestor` | `string` | `varchar` |  |  | UGR_REQUESTOR |
| `ugr_updatecount` | `float` | `float` |  |  | UGR_UPDATECOUNT |
| `ugr_created` | `timestamp` | `timestamp_ntz` |  |  | UGR_CREATED |
| `ugr_updated` | `timestamp` | `timestamp_ntz` |  |  | UGR_UPDATED |
| `ugr_sessiontimeout` | `float` | `float` |  |  | UGR_SESSIONTIMEOUT |
| `ugr_jobtype` | `string` | `varchar` |  |  | UGR_JOBTYPE |
| `ugr_addperm` | `string` | `varchar` |  |  | UGR_ADDPERM |
| `ugr_downloadworkorders` | `string` | `varchar` |  |  | UGR_DOWNLOADWORKORDERS |
| `ugr_fordataspy` | `float` | `float` |  |  | UGR_FORDATASPY |
| `ugr_downloademployee` | `string` | `varchar` |  |  | UGR_DOWNLOADEMPLOYEE |
| `ugr_fortrade` | `string` | `varchar` |  |  | UGR_FORTRADE |
| `ugr_fordepartment_emp` | `string` | `varchar` |  |  | UGR_FORDEPARTMENT_EMP |
| `ugr_foremployee` | `string` | `varchar` |  |  | UGR_FOREMPLOYEE |
| `ugr_downloadstandardwos` | `string` | `varchar` |  |  | UGR_DOWNLOADSTANDARDWOS |
| `ugr_downloadinspresults` | `string` | `varchar` |  |  | UGR_DOWNLOADINSPRESULTS |
| `ugr_downloadtask` | `string` | `varchar` |  |  | UGR_DOWNLOADTASK |
| `ugr_downloadequipment` | `string` | `varchar` |  |  | UGR_DOWNLOADEQUIPMENT |
| `ugr_fordepartment_equip` | `string` | `varchar` |  |  | UGR_FORDEPARTMENT_EQUIP |
| `ugr_forlocation` | `string` | `varchar` |  |  | UGR_FORLOCATION |
| `ugr_forclass_equip` | `string` | `varchar` |  |  | UGR_FORCLASS_EQUIP |
| `ugr_forcategory` | `string` | `varchar` |  |  | UGR_FORCATEGORY |
| `ugr_forequipment` | `string` | `varchar` |  |  | UGR_FOREQUIPMENT |
| `ugr_fortype` | `string` | `varchar` |  |  | UGR_FORTYPE |
| `ugr_downloadequipcomments` | `string` | `varchar` |  |  | UGR_DOWNLOADEQUIPCOMMENTS |
| `ugr_fordownloadedwo_eqcmt` | `string` | `varchar` |  |  | UGR_FORDOWNLOADEDWO_EQCMT |
| `ugr_downloadequiphistory` | `string` | `varchar` |  |  | UGR_DOWNLOADEQUIPHISTORY |
| `ugr_fordownloadedwo_eqhst` | `string` | `varchar` |  |  | UGR_FORDOWNLOADEDWO_EQHST |
| `ugr_forlatestdays` | `float` | `float` |  |  | UGR_FORLATESTDAYS |
| `ugr_downloadeqcustomfields` | `string` | `varchar` |  |  | UGR_DOWNLOADEQCUSTOMFIELDS |
| `ugr_fordownloadedwo_eqcf` | `string` | `varchar` |  |  | UGR_FORDOWNLOADEDWO_EQCF |
| `ugr_downloadcostcodes` | `string` | `varchar` |  |  | UGR_DOWNLOADCOSTCODES |
| `ugr_forclass_cc` | `string` | `varchar` |  |  | UGR_FORCLASS_CC |
| `ugr_forcostcode` | `string` | `varchar` |  |  | UGR_FORCOSTCODE |
| `ugr_downloadeqhistcomments` | `string` | `varchar` |  |  | UGR_DOWNLOADEQHISTCOMMENTS |
| `ugr_downloadstores` | `string` | `varchar` |  |  | UGR_DOWNLOADSTORES |
| `ugr_downloadparts` | `string` | `varchar` |  |  | UGR_DOWNLOADPARTS |
| `ugr_forstore_part` | `string` | `varchar` |  |  | UGR_FORSTORE_PART |
| `ugr_forpart_part` | `string` | `varchar` |  |  | UGR_FORPART_PART |
| `ugr_maxinstockandquantity` | `float` | `float` |  |  | UGR_MAXINSTOCKANDQUANTITY |
| `ugr_downloadbins` | `string` | `varchar` |  |  | UGR_DOWNLOADBINS |
| `ugr_forstore_bin` | `string` | `varchar` |  |  | UGR_FORSTORE_BIN |
| `ugr_forbin` | `string` | `varchar` |  |  | UGR_FORBIN |
| `ugr_downloadphyinventory` | `string` | `varchar` |  |  | UGR_DOWNLOADPHYINVENTORY |
| `ugr_forstore_inv` | `string` | `varchar` |  |  | UGR_FORSTORE_INV |
| `ugr_forline` | `string` | `varchar` |  |  | UGR_FORLINE |
| `ugr_downloadsupplier` | `string` | `varchar` |  |  | UGR_DOWNLOADSUPPLIER |
| `ugr_downloadassetinventory` | `string` | `varchar` |  |  | UGR_DOWNLOADASSETINVENTORY |
| `ugr_downloadstock` | `string` | `varchar` |  |  | UGR_DOWNLOADSTOCK |
| `ugr_downloadesignsettings` | `string` | `varchar` |  |  | UGR_DOWNLOADESIGNSETTINGS |
| `ugr_downloadmtrreading` | `string` | `varchar` |  |  | UGR_DOWNLOADMTRREADING |
| `ugr_downloaddocprintwo` | `string` | `varchar` |  |  | UGR_DOWNLOADDOCPRINTWO |
| `ugr_equipfordataspy` | `float` | `float` |  |  | UGR_EQUIPFORDATASPY |
| `ugr_mtrreadinglatestdays` | `float` | `float` |  |  | UGR_MTRREADINGLATESTDAYS |
| `ugr_code` | `string` | `varchar` | `PK` | `unique` | UGR_CODE |
| `ugr_desc` | `string` | `varchar` |  |  | UGR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
