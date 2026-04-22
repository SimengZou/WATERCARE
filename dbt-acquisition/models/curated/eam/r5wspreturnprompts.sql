{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSPRETURNPROMPTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WPR_LASTSAVED'], type='timestamp_ntz', alias='wpr_lastsaved') }},
        {{ trx_json_extract('src', ['WPR_SOURCESEQ'], type='float', alias='wpr_sourceseq') }},
        {{ trx_json_extract('src', ['WPR_TARGETSEQ'], type='float', alias='wpr_targetseq') }},
        {{ trx_json_extract('src', ['WPR_UPDATED'], type='timestamp_ntz', alias='wpr_updated') }},
        {{ trx_json_extract('src', ['WPR_UPDATECOUNT'], type='float', alias='wpr_updatecount') }},
        {{ trx_json_extract('src', ['WPR_MOBILEQUERYCODE'], type='varchar', alias='wpr_mobilequerycode') }},
        {{ trx_json_extract('src', ['WPR_WSPCODE'], type='varchar', alias='wpr_wspcode') }},
        {{ trx_json_extract('src', ['WPR_QUERYCODE'], type='varchar', alias='wpr_querycode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wspreturnprompts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wpr_wspcode', 'wpr_sourceseq', 'wpr_targetseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wpr_lastsaved']) }}
