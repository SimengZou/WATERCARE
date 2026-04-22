# r5precautions

eam_R5PRECAUTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5precautions` |
| **Materialization** | `incremental` |
| **Primary keys** | `pca_code`, `pca_org`, `pca_revision` |
| **Column count** | 74 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pca_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PCA_LASTSAVED |
| `pca_code` | `string` | `varchar` | `PK` |  | PCA_CODE |
| `pca_org` | `string` | `varchar` | `PK` |  | PCA_ORG |
| `pca_revision` | `float` | `float` | `PK` |  | PCA_REVISION |
| `pca_desc` | `string` | `varchar` |  |  | PCA_DESC |
| `pca_status` | `string` | `varchar` |  |  | PCA_STATUS |
| `pca_rstatus` | `string` | `varchar` |  |  | PCA_RSTATUS |
| `pca_notused` | `string` | `varchar` |  |  | PCA_NOTUSED |
| `pca_timing` | `string` | `varchar` |  |  | PCA_TIMING |
| `pca_class` | `string` | `varchar` |  |  | PCA_CLASS |
| `pca_class_org` | `string` | `varchar` |  |  | PCA_CLASS_ORG |
| `pca_reviewrequired` | `timestamp` | `timestamp_ntz` |  |  | PCA_REVIEWREQUIRED |
| `pca_created` | `timestamp` | `timestamp_ntz` |  |  | PCA_CREATED |
| `pca_createdby` | `string` | `varchar` |  |  | PCA_CREATEDBY |
| `pca_updated` | `timestamp` | `timestamp_ntz` |  |  | PCA_UPDATED |
| `pca_updatedby` | `string` | `varchar` |  |  | PCA_UPDATEDBY |
| `pca_requested` | `timestamp` | `timestamp_ntz` |  |  | PCA_REQUESTED |
| `pca_requestedby` | `string` | `varchar` |  |  | PCA_REQUESTEDBY |
| `pca_approved` | `timestamp` | `timestamp_ntz` |  |  | PCA_APPROVED |
| `pca_approvedby` | `string` | `varchar` |  |  | PCA_APPROVEDBY |
| `pca_revisionreason` | `string` | `varchar` |  |  | PCA_REVISIONREASON |
| `pca_udfchar01` | `string` | `varchar` |  |  | PCA_UDFCHAR01 |
| `pca_udfchar02` | `string` | `varchar` |  |  | PCA_UDFCHAR02 |
| `pca_udfchar03` | `string` | `varchar` |  |  | PCA_UDFCHAR03 |
| `pca_udfchar04` | `string` | `varchar` |  |  | PCA_UDFCHAR04 |
| `pca_udfchar05` | `string` | `varchar` |  |  | PCA_UDFCHAR05 |
| `pca_udfchar06` | `string` | `varchar` |  |  | PCA_UDFCHAR06 |
| `pca_udfchar07` | `string` | `varchar` |  |  | PCA_UDFCHAR07 |
| `pca_udfchar08` | `string` | `varchar` |  |  | PCA_UDFCHAR08 |
| `pca_udfchar09` | `string` | `varchar` |  |  | PCA_UDFCHAR09 |
| `pca_udfchar10` | `string` | `varchar` |  |  | PCA_UDFCHAR10 |
| `pca_udfchar11` | `string` | `varchar` |  |  | PCA_UDFCHAR11 |
| `pca_udfchar12` | `string` | `varchar` |  |  | PCA_UDFCHAR12 |
| `pca_udfchar13` | `string` | `varchar` |  |  | PCA_UDFCHAR13 |
| `pca_udfchar14` | `string` | `varchar` |  |  | PCA_UDFCHAR14 |
| `pca_udfchar15` | `string` | `varchar` |  |  | PCA_UDFCHAR15 |
| `pca_udfchar16` | `string` | `varchar` |  |  | PCA_UDFCHAR16 |
| `pca_udfchar17` | `string` | `varchar` |  |  | PCA_UDFCHAR17 |
| `pca_udfchar18` | `string` | `varchar` |  |  | PCA_UDFCHAR18 |
| `pca_udfchar19` | `string` | `varchar` |  |  | PCA_UDFCHAR19 |
| `pca_udfchar20` | `string` | `varchar` |  |  | PCA_UDFCHAR20 |
| `pca_udfchar21` | `string` | `varchar` |  |  | PCA_UDFCHAR21 |
| `pca_udfchar22` | `string` | `varchar` |  |  | PCA_UDFCHAR22 |
| `pca_udfchar23` | `string` | `varchar` |  |  | PCA_UDFCHAR23 |
| `pca_udfchar24` | `string` | `varchar` |  |  | PCA_UDFCHAR24 |
| `pca_udfchar25` | `string` | `varchar` |  |  | PCA_UDFCHAR25 |
| `pca_udfchar26` | `string` | `varchar` |  |  | PCA_UDFCHAR26 |
| `pca_udfchar27` | `string` | `varchar` |  |  | PCA_UDFCHAR27 |
| `pca_udfchar28` | `string` | `varchar` |  |  | PCA_UDFCHAR28 |
| `pca_udfchar29` | `string` | `varchar` |  |  | PCA_UDFCHAR29 |
| `pca_udfchar30` | `string` | `varchar` |  |  | PCA_UDFCHAR30 |
| `pca_udfnum01` | `float` | `float` |  |  | PCA_UDFNUM01 |
| `pca_udfnum02` | `float` | `float` |  |  | PCA_UDFNUM02 |
| `pca_udfnum03` | `float` | `float` |  |  | PCA_UDFNUM03 |
| `pca_udfnum04` | `float` | `float` |  |  | PCA_UDFNUM04 |
| `pca_udfnum05` | `float` | `float` |  |  | PCA_UDFNUM05 |
| `pca_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PCA_UDFDATE01 |
| `pca_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PCA_UDFDATE02 |
| `pca_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PCA_UDFDATE03 |
| `pca_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PCA_UDFDATE04 |
| `pca_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PCA_UDFDATE05 |
| `pca_udfchkbox01` | `string` | `varchar` |  |  | PCA_UDFCHKBOX01 |
| `pca_udfchkbox02` | `string` | `varchar` |  |  | PCA_UDFCHKBOX02 |
| `pca_udfchkbox03` | `string` | `varchar` |  |  | PCA_UDFCHKBOX03 |
| `pca_udfchkbox04` | `string` | `varchar` |  |  | PCA_UDFCHKBOX04 |
| `pca_udfchkbox05` | `string` | `varchar` |  |  | PCA_UDFCHKBOX05 |
| `pca_updatecount` | `float` | `float` |  |  | PCA_UPDATECOUNT |
| `pca_responsibility` | `string` | `varchar` |  |  | PCA_RESPONSIBILITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
