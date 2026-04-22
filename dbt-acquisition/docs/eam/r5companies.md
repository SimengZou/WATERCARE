# r5companies

eam_R5COMPANIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5companies` |
| **Materialization** | `incremental` |
| **Primary keys** | `com_code`, `com_org` |
| **Column count** | 99 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `com_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | COM_LASTSAVED |
| `com_code` | `string` | `varchar` | `PK` |  | COM_CODE |
| `com_desc` | `string` | `varchar` |  |  | COM_DESC |
| `com_class` | `string` | `varchar` |  |  | COM_CLASS |
| `com_contact` | `string` | `varchar` |  |  | COM_CONTACT |
| `com_ourcont` | `string` | `varchar` |  |  | COM_OURCONT |
| `com_curr` | `string` | `varchar` |  |  | COM_CURR |
| `com_lang` | `string` | `varchar` |  |  | COM_LANG |
| `com_leadtime` | `float` | `float` |  |  | COM_LEADTIME |
| `com_maxord` | `float` | `float` |  |  | COM_MAXORD |
| `com_minord` | `float` | `float` |  |  | COM_MINORD |
| `com_status` | `string` | `varchar` |  |  | COM_STATUS |
| `com_phone` | `string` | `varchar` |  |  | COM_PHONE |
| `com_fax` | `string` | `varchar` |  |  | COM_FAX |
| `com_buyer` | `string` | `varchar` |  |  | COM_BUYER |
| `com_parent` | `string` | `varchar` |  |  | COM_PARENT |
| `com_paymentterms` | `string` | `varchar` |  |  | COM_PAYMENTTERMS |
| `com_freightterms` | `string` | `varchar` |  |  | COM_FREIGHTTERMS |
| `com_shipvia` | `string` | `varchar` |  |  | COM_SHIPVIA |
| `com_fobpoint` | `string` | `varchar` |  |  | COM_FOBPOINT |
| `com_purchasesite` | `string` | `varchar` |  |  | COM_PURCHASESITE |
| `com_minoritygroup` | `string` | `varchar` |  |  | COM_MINORITYGROUP |
| `com_edino` | `string` | `varchar` |  |  | COM_EDINO |
| `com_sourcesystem` | `string` | `varchar` |  |  | COM_SOURCESYSTEM |
| `com_sourcecode` | `string` | `varchar` |  |  | COM_SOURCECODE |
| `com_interface` | `timestamp` | `timestamp_ntz` |  |  | COM_INTERFACE |
| `com_notused` | `string` | `varchar` |  |  | COM_NOTUSED |
| `com_capacity` | `float` | `float` |  |  | COM_CAPACITY |
| `com_people` | `string` | `varchar` |  |  | COM_PEOPLE |
| `com_org` | `string` | `varchar` | `PK` |  | COM_ORG |
| `com_class_org` | `string` | `varchar` |  |  | COM_CLASS_ORG |
| `com_parent_org` | `string` | `varchar` |  |  | COM_PARENT_ORG |
| `com_email` | `string` | `varchar` |  |  | COM_EMAIL |
| `com_ipvendor` | `float` | `float` |  |  | COM_IPVENDOR |
| `com_paybymethod` | `string` | `varchar` |  |  | COM_PAYBYMETHOD |
| `com_ipaccount` | `string` | `varchar` |  |  | COM_IPACCOUNT |
| `com_ipresponse` | `string` | `varchar` |  |  | COM_IPRESPONSE |
| `com_updatecount` | `float` | `float` |  |  | COM_UPDATECOUNT |
| `com_created` | `timestamp` | `timestamp_ntz` |  |  | COM_CREATED |
| `com_updated` | `timestamp` | `timestamp_ntz` |  |  | COM_UPDATED |
| `com_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | COM_LASTSTATUSUPDATE |
| `com_udfchar01` | `string` | `varchar` |  |  | COM_UDFCHAR01 |
| `com_udfchar02` | `string` | `varchar` |  |  | COM_UDFCHAR02 |
| `com_udfchar03` | `string` | `varchar` |  |  | COM_UDFCHAR03 |
| `com_type1099` | `string` | `varchar` |  |  | COM_TYPE1099 |
| `com_udfchar04` | `string` | `varchar` |  |  | COM_UDFCHAR04 |
| `com_udfchar05` | `string` | `varchar` |  |  | COM_UDFCHAR05 |
| `com_udfchar06` | `string` | `varchar` |  |  | COM_UDFCHAR06 |
| `com_udfchar07` | `string` | `varchar` |  |  | COM_UDFCHAR07 |
| `com_udfchar08` | `string` | `varchar` |  |  | COM_UDFCHAR08 |
| `com_udfchar09` | `string` | `varchar` |  |  | COM_UDFCHAR09 |
| `com_udfchar10` | `string` | `varchar` |  |  | COM_UDFCHAR10 |
| `com_udfchar11` | `string` | `varchar` |  |  | COM_UDFCHAR11 |
| `com_udfchar12` | `string` | `varchar` |  |  | COM_UDFCHAR12 |
| `com_udfchar13` | `string` | `varchar` |  |  | COM_UDFCHAR13 |
| `com_udfchar14` | `string` | `varchar` |  |  | COM_UDFCHAR14 |
| `com_udfchar15` | `string` | `varchar` |  |  | COM_UDFCHAR15 |
| `com_udfchar16` | `string` | `varchar` |  |  | COM_UDFCHAR16 |
| `com_udfchar17` | `string` | `varchar` |  |  | COM_UDFCHAR17 |
| `com_udfchar18` | `string` | `varchar` |  |  | COM_UDFCHAR18 |
| `com_udfchar19` | `string` | `varchar` |  |  | COM_UDFCHAR19 |
| `com_udfchar20` | `string` | `varchar` |  |  | COM_UDFCHAR20 |
| `com_udfchar21` | `string` | `varchar` |  |  | COM_UDFCHAR21 |
| `com_udfchar22` | `string` | `varchar` |  |  | COM_UDFCHAR22 |
| `com_udfchar23` | `string` | `varchar` |  |  | COM_UDFCHAR23 |
| `com_udfchar24` | `string` | `varchar` |  |  | COM_UDFCHAR24 |
| `com_udfchar25` | `string` | `varchar` |  |  | COM_UDFCHAR25 |
| `com_udfchar26` | `string` | `varchar` |  |  | COM_UDFCHAR26 |
| `com_udfchar27` | `string` | `varchar` |  |  | COM_UDFCHAR27 |
| `com_udfchar28` | `string` | `varchar` |  |  | COM_UDFCHAR28 |
| `com_udfchar29` | `string` | `varchar` |  |  | COM_UDFCHAR29 |
| `com_udfchar30` | `string` | `varchar` |  |  | COM_UDFCHAR30 |
| `com_udfnum01` | `float` | `float` |  |  | COM_UDFNUM01 |
| `com_udfnum02` | `float` | `float` |  |  | COM_UDFNUM02 |
| `com_udfnum03` | `float` | `float` |  |  | COM_UDFNUM03 |
| `com_udfnum04` | `float` | `float` |  |  | COM_UDFNUM04 |
| `com_udfnum05` | `float` | `float` |  |  | COM_UDFNUM05 |
| `com_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | COM_UDFDATE01 |
| `com_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | COM_UDFDATE02 |
| `com_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | COM_UDFDATE03 |
| `com_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | COM_UDFDATE04 |
| `com_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | COM_UDFDATE05 |
| `com_udfchkbox01` | `string` | `varchar` |  |  | COM_UDFCHKBOX01 |
| `com_udfchkbox02` | `string` | `varchar` |  |  | COM_UDFCHKBOX02 |
| `com_udfchkbox03` | `string` | `varchar` |  |  | COM_UDFCHKBOX03 |
| `com_udfchkbox04` | `string` | `varchar` |  |  | COM_UDFCHKBOX04 |
| `com_costcenter` | `string` | `varchar` |  |  | COM_COSTCENTER |
| `com_udfchkbox05` | `string` | `varchar` |  |  | COM_UDFCHKBOX05 |
| `com_customer` | `string` | `varchar` |  |  | COM_CUSTOMER |
| `com_accountcode` | `string` | `varchar` |  |  | COM_ACCOUNTCODE |
| `com_tax` | `string` | `varchar` |  |  | COM_TAX |
| `com_grouppurchasingorg1` | `string` | `varchar` |  |  | COM_GROUPPURCHASINGORG1 |
| `com_grouppurchasingorg2` | `string` | `varchar` |  |  | COM_GROUPPURCHASINGORG2 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
