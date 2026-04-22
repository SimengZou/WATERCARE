{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5QUERYFIELD',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DQF_LASTSAVED'], type='timestamp_ntz', alias='dqf_lastsaved') }},
        {{ trx_json_extract('src', ['DQF_FIELDID'], type='float', alias='dqf_fieldid') }},
        {{ trx_json_extract('src', ['DQF_COLUMNWIDTH'], type='varchar', alias='dqf_columnwidth') }},
        {{ trx_json_extract('src', ['DQF_UPDATECOUNT'], type='float', alias='dqf_updatecount') }},
        {{ trx_json_extract('src', ['DQF_VIEWTYPE'], type='varchar', alias='dqf_viewtype') }},
        {{ trx_json_extract('src', ['DQF_UPDATED'], type='timestamp_ntz', alias='dqf_updated') }},
        {{ trx_json_extract('src', ['DQF_DDSPYID'], type='float', alias='dqf_ddspyid') }},
        {{ trx_json_extract('src', ['DQF_COLUMNORDER'], type='float', alias='dqf_columnorder') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5queryfield') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dqf_fieldid', 'dqf_ddspyid', 'dqf_viewtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dqf_lastsaved']) }}
