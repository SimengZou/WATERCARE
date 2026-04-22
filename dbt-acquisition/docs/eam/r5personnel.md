# r5personnel

eam_R5PERSONNEL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5personnel` |
| **Materialization** | `incremental` |
| **Primary keys** | `per_code` |
| **Column count** | 107 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `per_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PER_LASTSAVED |
| `per_code` | `string` | `varchar` | `PK` | `unique` | PER_CODE |
| `per_desc` | `string` | `varchar` |  |  | PER_DESC |
| `per_class` | `string` | `varchar` |  |  | PER_CLASS |
| `per_mrc` | `string` | `varchar` |  |  | PER_MRC |
| `per_trade` | `string` | `varchar` |  |  | PER_TRADE |
| `per_notused` | `string` | `varchar` |  |  | PER_NOTUSED |
| `per_capacity` | `float` | `float` |  |  | PER_CAPACITY |
| `per_org` | `string` | `varchar` |  |  | PER_ORG |
| `per_class_org` | `string` | `varchar` |  |  | PER_CLASS_ORG |
| `per_emailaddress` | `string` | `varchar` |  |  | PER_EMAILADDRESS |
| `per_notificationpref` | `string` | `varchar` |  |  | PER_NOTIFICATIONPREF |
| `per_user` | `string` | `varchar` |  |  | PER_USER |
| `per_created` | `timestamp` | `timestamp_ntz` |  |  | PER_CREATED |
| `per_createdby` | `string` | `varchar` |  |  | PER_CREATEDBY |
| `per_updated` | `timestamp` | `timestamp_ntz` |  |  | PER_UPDATED |
| `per_updatedby` | `string` | `varchar` |  |  | PER_UPDATEDBY |
| `per_updatecount` | `float` | `float` |  |  | PER_UPDATECOUNT |
| `per_fleetcustomer` | `string` | `varchar` |  |  | PER_FLEETCUSTOMER |
| `per_fleetcustomer_org` | `string` | `varchar` |  |  | PER_FLEETCUSTOMER_ORG |
| `per_costcode` | `string` | `varchar` |  |  | PER_COSTCODE |
| `per_driverslicense` | `string` | `varchar` |  |  | PER_DRIVERSLICENSE |
| `per_phone` | `string` | `varchar` |  |  | PER_PHONE |
| `per_parent` | `string` | `varchar` |  |  | PER_PARENT |
| `per_schedulingsessionid` | `string` | `varchar` |  |  | PER_SCHEDULINGSESSIONID |
| `per_schedulingtype` | `string` | `varchar` |  |  | PER_SCHEDULINGTYPE |
| `per_customer` | `string` | `varchar` |  |  | PER_CUSTOMER |
| `per_customer_org` | `string` | `varchar` |  |  | PER_CUSTOMER_ORG |
| `per_alias` | `string` | `varchar` |  |  | PER_ALIAS |
| `per_object` | `string` | `varchar` |  |  | PER_OBJECT |
| `per_object_org` | `string` | `varchar` |  |  | PER_OBJECT_ORG |
| `per_udfchar01` | `string` | `varchar` |  |  | PER_UDFCHAR01 |
| `per_udfchar02` | `string` | `varchar` |  |  | PER_UDFCHAR02 |
| `per_udfchar03` | `string` | `varchar` |  |  | PER_UDFCHAR03 |
| `per_udfchar04` | `string` | `varchar` |  |  | PER_UDFCHAR04 |
| `per_udfchar05` | `string` | `varchar` |  |  | PER_UDFCHAR05 |
| `per_udfchar06` | `string` | `varchar` |  |  | PER_UDFCHAR06 |
| `per_udfchar07` | `string` | `varchar` |  |  | PER_UDFCHAR07 |
| `per_udfchar08` | `string` | `varchar` |  |  | PER_UDFCHAR08 |
| `per_udfchar09` | `string` | `varchar` |  |  | PER_UDFCHAR09 |
| `per_udfchar10` | `string` | `varchar` |  |  | PER_UDFCHAR10 |
| `per_udfchar11` | `string` | `varchar` |  |  | PER_UDFCHAR11 |
| `per_udfchar12` | `string` | `varchar` |  |  | PER_UDFCHAR12 |
| `per_udfchar13` | `string` | `varchar` |  |  | PER_UDFCHAR13 |
| `per_udfchar14` | `string` | `varchar` |  |  | PER_UDFCHAR14 |
| `per_udfchar15` | `string` | `varchar` |  |  | PER_UDFCHAR15 |
| `per_udfchar16` | `string` | `varchar` |  |  | PER_UDFCHAR16 |
| `per_udfchar17` | `string` | `varchar` |  |  | PER_UDFCHAR17 |
| `per_udfchar18` | `string` | `varchar` |  |  | PER_UDFCHAR18 |
| `per_udfchar19` | `string` | `varchar` |  |  | PER_UDFCHAR19 |
| `per_udfchar20` | `string` | `varchar` |  |  | PER_UDFCHAR20 |
| `per_udfchar21` | `string` | `varchar` |  |  | PER_UDFCHAR21 |
| `per_udfchar22` | `string` | `varchar` |  |  | PER_UDFCHAR22 |
| `per_udfchar23` | `string` | `varchar` |  |  | PER_UDFCHAR23 |
| `per_udfchar24` | `string` | `varchar` |  |  | PER_UDFCHAR24 |
| `per_udfchar25` | `string` | `varchar` |  |  | PER_UDFCHAR25 |
| `per_udfchar26` | `string` | `varchar` |  |  | PER_UDFCHAR26 |
| `per_udfchar27` | `string` | `varchar` |  |  | PER_UDFCHAR27 |
| `per_udfchar28` | `string` | `varchar` |  |  | PER_UDFCHAR28 |
| `per_udfchar29` | `string` | `varchar` |  |  | PER_UDFCHAR29 |
| `per_udfchar30` | `string` | `varchar` |  |  | PER_UDFCHAR30 |
| `per_udfnum01` | `float` | `float` |  |  | PER_UDFNUM01 |
| `per_udfnum02` | `float` | `float` |  |  | PER_UDFNUM02 |
| `per_udfnum03` | `float` | `float` |  |  | PER_UDFNUM03 |
| `per_udfnum04` | `float` | `float` |  |  | PER_UDFNUM04 |
| `per_udfnum05` | `float` | `float` |  |  | PER_UDFNUM05 |
| `per_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PER_UDFDATE01 |
| `per_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PER_UDFDATE02 |
| `per_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PER_UDFDATE03 |
| `per_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PER_UDFDATE04 |
| `per_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PER_UDFDATE05 |
| `per_udfchkbox01` | `string` | `varchar` |  |  | PER_UDFCHKBOX01 |
| `per_udfchkbox02` | `string` | `varchar` |  |  | PER_UDFCHKBOX02 |
| `per_udfchkbox03` | `string` | `varchar` |  |  | PER_UDFCHKBOX03 |
| `per_udfchkbox04` | `string` | `varchar` |  |  | PER_UDFCHKBOX04 |
| `per_udfchkbox05` | `string` | `varchar` |  |  | PER_UDFCHKBOX05 |
| `per_jobtitle` | `string` | `varchar` |  |  | PER_JOBTITLE |
| `per_supervisor` | `string` | `varchar` |  |  | PER_SUPERVISOR |
| `per_hiredate` | `timestamp` | `timestamp_ntz` |  |  | PER_HIREDATE |
| `per_birthdate` | `timestamp` | `timestamp_ntz` |  |  | PER_BIRTHDATE |
| `per_terminateddate` | `timestamp` | `timestamp_ntz` |  |  | PER_TERMINATEDDATE |
| `per_payrollno` | `string` | `varchar` |  |  | PER_PAYROLLNO |
| `per_securitybadgeno` | `string` | `varchar` |  |  | PER_SECURITYBADGENO |
| `per_address` | `string` | `varchar` |  |  | PER_ADDRESS |
| `per_city` | `string` | `varchar` |  |  | PER_CITY |
| `per_state` | `string` | `varchar` |  |  | PER_STATE |
| `per_zip` | `string` | `varchar` |  |  | PER_ZIP |
| `per_country` | `string` | `varchar` |  |  | PER_COUNTRY |
| `per_emergencycontact` | `string` | `varchar` |  |  | PER_EMERGENCYCONTACT |
| `per_emergencycontactphno` | `string` | `varchar` |  |  | PER_EMERGENCYCONTACTPHNO |
| `per_mobilephoneno` | `string` | `varchar` |  |  | PER_MOBILEPHONENO |
| `per_homephoneno` | `string` | `varchar` |  |  | PER_HOMEPHONENO |
| `per_url` | `string` | `varchar` |  |  | PER_URL |
| `per_fax` | `string` | `varchar` |  |  | PER_FAX |
| `per_initials` | `string` | `varchar` |  |  | PER_INITIALS |
| `per_tradesperson` | `string` | `varchar` |  |  | PER_TRADESPERSON |
| `per_workspaceoccupant` | `string` | `varchar` |  |  | PER_WORKSPACEOCCUPANT |
| `per_profilepicture` | `string` | `varchar` |  |  | PER_PROFILEPICTURE |
| `per_determinescrewlocation` | `string` | `varchar` |  |  | PER_DETERMINESCREWLOCATION |
| `per_startingxcoordinate` | `float` | `float` |  |  | PER_STARTINGXCOORDINATE |
| `per_startingycoordinate` | `float` | `float` |  |  | PER_STARTINGYCOORDINATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
