{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Generated from anySQL Connector dEPM_R100Reporting',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['Version'], type='varchar', alias='version') }},
        {{ trx_json_extract('src', ['ProjectCode'], type='varchar', alias='projectcode') }},
        {{ trx_json_extract('src', ['ProjectName'], type='varchar', alias='projectname') }},
        {{ trx_json_extract('src', ['WBSCode'], type='varchar', alias='wbscode') }},
        {{ trx_json_extract('src', ['WBSName'], type='varchar', alias='wbsname') }},
        {{ trx_json_extract('src', ['CategoryCode'], type='varchar', alias='categorycode') }},
        {{ trx_json_extract('src', ['CategoryName'], type='varchar', alias='categoryname') }},
        {{ trx_json_extract('src', ['Type'], type='varchar', alias='type') }},
        {{ trx_json_extract('src', ['RiskID'], type='varchar', alias='riskid') }},
        {{ trx_json_extract('src', ['RiskActionID'], type='varchar', alias='riskactionid') }},
        {{ trx_json_extract('src', ['R_RiskNum'], type='varchar', alias='r_risknum') }},
        {{ trx_json_extract('src', ['R_Title'], type='varchar', alias='r_title') }},
        {{ trx_json_extract('src', ['R_Description'], type='varchar', alias='r_description') }},
        {{ trx_json_extract('src', ['R_Cause'], type='varchar', alias='r_cause') }},
        {{ trx_json_extract('src', ['R_DueDate'], type='timestamp_ntz', alias='r_duedate') }},
        {{ trx_json_extract('src', ['R_Owner'], type='varchar', alias='r_owner') }},
        {{ trx_json_extract('src', ['R_Status_Msg'], type='varchar', alias='r_status_msg') }},
        {{ trx_json_extract('src', ['R_Status'], type='varchar', alias='r_status') }},
        {{ trx_json_extract('src', ['R_Impact_Rank'], type='varchar', alias='r_impact_rank') }},
        {{ trx_json_extract('src', ['R_Impact_Cost'], type='varchar', alias='r_impact_cost') }},
        {{ trx_json_extract('src', ['R_Risk_Cost'], type='varchar', alias='r_risk_cost') }},
        {{ trx_json_extract('src', ['R_Probability_Rate'], type='varchar', alias='r_probability_rate') }},
        {{ trx_json_extract('src', ['R_Probability_Rank'], type='varchar', alias='r_probability_rank') }},
        {{ trx_json_extract('src', ['R_Risk_Rank'], type='varchar', alias='r_risk_rank') }},
        {{ trx_json_extract('src', ['R_Impact_Time'], type='varchar', alias='r_impact_time') }},
        {{ trx_json_extract('src', ['R_ActionsTotal'], type='varchar', alias='r_actionstotal') }},
        {{ trx_json_extract('src', ['R_ActionsCompleted'], type='varchar', alias='r_actionscompleted') }},
        {{ trx_json_extract('src', ['R_Treatment'], type='varchar', alias='r_treatment') }},
        {{ trx_json_extract('src', ['R_IsIssue'], type='varchar', alias='r_isissue') }},
        {{ trx_json_extract('src', ['R_Mitig_Cost'], type='varchar', alias='r_mitig_cost') }},
        {{ trx_json_extract('src', ['R_Mitig_Impact_Rank'], type='varchar', alias='r_mitig_impact_rank') }},
        {{ trx_json_extract('src', ['R_Mitig_Impact_Cost'], type='varchar', alias='r_mitig_impact_cost') }},
        {{ trx_json_extract('src', ['R_Mitig_Risk_Cost'], type='varchar', alias='r_mitig_risk_cost') }},
        {{ trx_json_extract('src', ['R_Mitig_Probability_Rate'], type='varchar', alias='r_mitig_probability_rate') }},
        {{ trx_json_extract('src', ['R_Mitig_Probability_Rank'], type='varchar', alias='r_mitig_probability_rank') }},
        {{ trx_json_extract('src', ['R_Mitig_Risk_Rank'], type='varchar', alias='r_mitig_risk_rank') }},
        {{ trx_json_extract('src', ['R_Mitig_Impact_Time'], type='varchar', alias='r_mitig_impact_time') }},
        {{ trx_json_extract('src', ['R_Mitig_Status'], type='varchar', alias='r_mitig_status') }},
        {{ trx_json_extract('src', ['R_Mitig_Owner'], type='varchar', alias='r_mitig_owner') }},
        {{ trx_json_extract('src', ['R_Mitig_DueDate'], type='timestamp_ntz', alias='r_mitig_duedate') }},
        {{ trx_json_extract('src', ['R_Mitig_Desc'], type='varchar', alias='r_mitig_desc') }},
        {{ trx_json_extract('src', ['I_Description'], type='varchar', alias='i_description') }},
        {{ trx_json_extract('src', ['I_Impact_Cost'], type='varchar', alias='i_impact_cost') }},
        {{ trx_json_extract('src', ['I_Owner'], type='varchar', alias='i_owner') }},
        {{ trx_json_extract('src', ['I_Priority'], type='varchar', alias='i_priority') }},
        {{ trx_json_extract('src', ['I_Impact_Time'], type='varchar', alias='i_impact_time') }},
        {{ trx_json_extract('src', ['I_Impact_Rank'], type='varchar', alias='i_impact_rank') }},
        {{ trx_json_extract('src', ['I_DueDate'], type='timestamp_ntz', alias='i_duedate') }},
        {{ trx_json_extract('src', ['I_Actions'], type='varchar', alias='i_actions') }},
        {{ trx_json_extract('src', ['I_Status'], type='varchar', alias='i_status') }},
        {{ trx_json_extract('src', ['R_InUse'], type='varchar', alias='r_inuse') }},
        {{ trx_json_extract('src', ['R_NotInUse'], type='varchar', alias='r_notinuse') }},
        {{ trx_json_extract('src', ['R_Mitig_NotInUse'], type='varchar', alias='r_mitig_notinuse') }},
        {{ trx_json_extract('src', ['R_Mitig_InUse'], type='varchar', alias='r_mitig_inuse') }},
        {{ trx_json_extract('src', ['I_DataChanged'], type='varchar', alias='i_datachanged') }},
        {{ trx_json_extract('src', ['TimeStamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_r100reporting') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['version', 'projectcode', 'wbscode', 'categorycode', 'riskid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}
