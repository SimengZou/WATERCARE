# r100reporting_temp

Generated from anySQL Connector dEPM_R100Reporting_Temp

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_r100reporting_temp` |
| **Materialization** | `incremental` |
| **Primary keys** | `version`, `projectcode`, `wbscode`, `categorycode`, `riskid` |
| **Column count** | 61 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `version` | `string` | `varchar` | `PK` |  | From table depm_R100_Reporting and column Version. Max length: 100 |
| `projectcode` | `string` | `varchar` | `PK` |  | From table depm_R100_Reporting and column ProjectCode. Max length: 100 |
| `projectname` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column ProjectName. Max length: 100 |
| `wbscode` | `string` | `varchar` | `PK` |  | From table depm_R100_Reporting and column WBSCode. Max length: 100 |
| `wbsname` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column WBSName. Max length: 100 |
| `categorycode` | `string` | `varchar` | `PK` |  | From table depm_R100_Reporting and column CategoryCode. Max length: 100 |
| `categoryname` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column CategoryName. Max length: 100 |
| `type` | `string` | `varchar` |  | `not_null` | (required) From table depm_R100_Reporting and column Type. Max length: 5 |
| `riskid` | `string` | `varchar` | `PK` |  | From table depm_R100_Reporting and column RiskID. Max length: 100 |
| `riskactionid` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column RiskActionID. Max length: 100 |
| `r_risknum` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_RiskNum. Max length: 100 |
| `r_title` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Title. Max length: 100 |
| `r_description` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Description. Max length: 100 |
| `r_cause` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Cause. Max length: 100 |
| `r_duedate` | `timestamp` | `timestamp_ntz` |  |  | From table depm_R100_Reporting and column R_DueDate. Max length: 24 |
| `r_owner` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Owner. Max length: 100 |
| `r_status_msg` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Status_Msg. Max length: 100 |
| `r_status` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Status. Max length: 100 |
| `r_impact_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Impact_Rank. Max length: 100 |
| `r_impact_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Impact_Cost. Max length: 100 |
| `r_risk_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Risk_Cost. Max length: 100 |
| `r_probability_rate` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Probability_Rate. Max length: 100 |
| `r_probability_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Probability_Rank. Max length: 100 |
| `r_risk_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Risk_Rank. Max length: 100 |
| `r_impact_time` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Impact_Time. Max length: 100 |
| `r_actionstotal` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_ActionsTotal. Max length: 100 |
| `r_actionscompleted` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_ActionsCompleted. Max length: 100 |
| `r_treatment` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Treatment. Max length: 100 |
| `r_isissue` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_IsIssue. Max length: 100 |
| `r_mitig_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Cost. Max length: 100 |
| `r_mitig_impact_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Impact_Rank. Max length: 100 |
| `r_mitig_impact_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Impact_Cost. Max length: 100 |
| `r_mitig_risk_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Risk_Cost. Max length: 100 |
| `r_mitig_probability_rate` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Probability_Rate. Max length: 100 |
| `r_mitig_probability_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Probability_Rank. Max length: 100 |
| `r_mitig_risk_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Risk_Rank. Max length: 100 |
| `r_mitig_impact_time` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Impact_Time. Max length: 100 |
| `r_mitig_status` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Status. Max length: 100 |
| `r_mitig_owner` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Owner. Max length: 100 |
| `r_mitig_duedate` | `timestamp` | `timestamp_ntz` |  |  | From table depm_R100_Reporting and column R_Mitig_DueDate. Max length: 24 |
| `r_mitig_desc` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_Desc. Max length: 100 |
| `i_description` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Description. Max length: 100 |
| `i_impact_cost` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Impact_Cost. Max length: 100 |
| `i_owner` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Owner. Max length: 100 |
| `i_priority` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Priority. Max length: 100 |
| `i_impact_time` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Impact_Time. Max length: 100 |
| `i_impact_rank` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Impact_Rank. Max length: 100 |
| `i_duedate` | `timestamp` | `timestamp_ntz` |  |  | From table depm_R100_Reporting and column I_DueDate. Max length: 24 |
| `i_actions` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Actions. Max length: 100 |
| `i_status` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_Status. Max length: 100 |
| `r_inuse` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_InUse. Max length: 100 |
| `r_notinuse` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_NotInUse. Max length: 100 |
| `r_mitig_notinuse` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_NotInUse. Max length: 100 |
| `r_mitig_inuse` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column R_Mitig_InUse. Max length: 100 |
| `i_datachanged` | `string` | `varchar` |  |  | From table depm_R100_Reporting and column I_DataChanged. Max length: 100 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  | From table depm_R100_Reporting and column TimeStamp. Max length: 24 |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
