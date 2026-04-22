# r5contactcenteroptions

eam_R5CONTACTCENTEROPTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5contactcenteroptions` |
| **Materialization** | `incremental` |
| **Primary keys** | `cop_org` |
| **Column count** | 52 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cop_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | COP_LASTSAVED |
| `cop_includepmworkorders` | `string` | `varchar` |  |  | COP_INCLUDEPMWORKORDERS |
| `cop_defcontactsource` | `string` | `varchar` |  |  | COP_DEFCONTACTSOURCE |
| `cop_defwostatus` | `string` | `varchar` |  |  | COP_DEFWOSTATUS |
| `cop_defopenstatus` | `string` | `varchar` |  |  | COP_DEFOPENSTATUS |
| `cop_deffollowupstatus` | `string` | `varchar` |  |  | COP_DEFFOLLOWUPSTATUS |
| `cop_toptenlookback` | `float` | `float` |  |  | COP_TOPTENLOOKBACK |
| `cop_closeable` | `string` | `varchar` |  |  | COP_CLOSEABLE |
| `cop_copycustomerinfo` | `string` | `varchar` |  |  | COP_COPYCUSTOMERINFO |
| `cop_woclosedays` | `float` | `float` |  |  | COP_WOCLOSEDAYS |
| `cop_deftype` | `string` | `varchar` |  |  | COP_DEFTYPE |
| `cop_deffindby` | `string` | `varchar` |  |  | COP_DEFFINDBY |
| `cop_updated` | `timestamp` | `timestamp_ntz` |  |  | COP_UPDATED |
| `cop_updatedby` | `string` | `varchar` |  |  | COP_UPDATEDBY |
| `cop_highlightmap` | `string` | `varchar` |  |  | COP_HIGHLIGHTMAP |
| `cop_idefeature` | `string` | `varchar` |  |  | COP_IDEFEATURE |
| `cop_showchildren` | `string` | `varchar` |  |  | COP_SHOWCHILDREN |
| `cop_nearaddress` | `string` | `varchar` |  |  | COP_NEARADDRESS |
| `cop_schedulewo` | `string` | `varchar` |  |  | COP_SCHEDULEWO |
| `cop_showprovider` | `string` | `varchar` |  |  | COP_SHOWPROVIDER |
| `cop_showservicecategory` | `string` | `varchar` |  |  | COP_SHOWSERVICECATEGORY |
| `cop_kbresultsperpage` | `float` | `float` |  |  | COP_KBRESULTSPERPAGE |
| `cop_updatecount` | `float` | `float` |  |  | COP_UPDATECOUNT |
| `cop_newcustomerallowed` | `string` | `varchar` |  |  | COP_NEWCUSTOMERALLOWED |
| `cop_minpenalty` | `float` | `float` |  |  | COP_MINPENALTY |
| `cop_deptstructureusage` | `string` | `varchar` |  |  | COP_DEPTSTRUCTUREUSAGE |
| `cop_deforgusage` | `string` | `varchar` |  |  | COP_DEFORGUSAGE |
| `cop_spcvalidation` | `string` | `varchar` |  |  | COP_SPCVALIDATION |
| `cop_eventtypefilter` | `string` | `varchar` |  |  | COP_EVENTTYPEFILTER |
| `cop_woduplicatecheck` | `string` | `varchar` |  |  | COP_WODUPLICATECHECK |
| `cop_woopendays` | `float` | `float` |  |  | COP_WOOPENDAYS |
| `cop_wotypes` | `string` | `varchar` |  |  | COP_WOTYPES |
| `cop_wostatuses` | `string` | `varchar` |  |  | COP_WOSTATUSES |
| `cop_woheaderobjonly` | `string` | `varchar` |  |  | COP_WOHEADEROBJONLY |
| `cop_matchsc` | `string` | `varchar` |  |  | COP_MATCHSC |
| `cop_matchspc` | `string` | `varchar` |  |  | COP_MATCHSPC |
| `cop_showspc` | `string` | `varchar` |  |  | COP_SHOWSPC |
| `cop_showsupplier` | `string` | `varchar` |  |  | COP_SHOWSUPPLIER |
| `cop_chkduplicateopenreq` | `string` | `varchar` |  |  | COP_CHKDUPLICATEOPENREQ |
| `cop_chkrecurringclosedreq` | `string` | `varchar` |  |  | COP_CHKRECURRINGCLOSEDREQ |
| `cop_recurringreqlookbackdays` | `float` | `float` |  |  | COP_RECURRINGREQLOOKBACKDAYS |
| `cop_defportalsource` | `string` | `varchar` |  |  | COP_DEFPORTALSOURCE |
| `cop_defportaltype` | `string` | `varchar` |  |  | COP_DEFPORTALTYPE |
| `cop_defportalstatus` | `string` | `varchar` |  |  | COP_DEFPORTALSTATUS |
| `cop_org` | `string` | `varchar` | `PK` | `unique` | COP_ORG |
| `cop_defwoorg` | `string` | `varchar` |  |  | COP_DEFWOORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
