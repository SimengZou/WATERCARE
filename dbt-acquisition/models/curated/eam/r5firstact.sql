{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FIRSTACT',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ACT_LASTSAVED'], type='timestamp_ntz', alias='act_lastsaved') }},
        {{ trx_json_extract('src', ['ACT_COMPLOCATION'], type='varchar', alias='act_complocation') }},
        {{ trx_json_extract('src', ['ACT_START'], type='timestamp_ntz', alias='act_start') }},
        {{ trx_json_extract('src', ['ACT_TRADE'], type='varchar', alias='act_trade') }},
        {{ trx_json_extract('src', ['ACT_PERSONS'], type='float', alias='act_persons') }},
        {{ trx_json_extract('src', ['ACT_DURATION'], type='float', alias='act_duration') }},
        {{ trx_json_extract('src', ['ACT_EST'], type='float', alias='act_est') }},
        {{ trx_json_extract('src', ['ACT_REM'], type='float', alias='act_rem') }},
        {{ trx_json_extract('src', ['ACT_TASK'], type='varchar', alias='act_task') }},
        {{ trx_json_extract('src', ['ACT_MATLIST'], type='varchar', alias='act_matlist') }},
        {{ trx_json_extract('src', ['ACT_MULTIPLETRADES'], type='varchar', alias='act_multipletrades') }},
        {{ trx_json_extract('src', ['ACT_RPC'], type='varchar', alias='act_rpc') }},
        {{ trx_json_extract('src', ['ACT_WAP'], type='varchar', alias='act_wap') }},
        {{ trx_json_extract('src', ['ACT_TPF'], type='varchar', alias='act_tpf') }},
        {{ trx_json_extract('src', ['ACT_MANUFACT'], type='varchar', alias='act_manufact') }},
        {{ trx_json_extract('src', ['ACT_SYSLEVEL'], type='varchar', alias='act_syslevel') }},
        {{ trx_json_extract('src', ['ACT_ASMLEVEL'], type='varchar', alias='act_asmlevel') }},
        {{ trx_json_extract('src', ['ACT_COMPLEVEL'], type='varchar', alias='act_complevel') }},
        {{ trx_json_extract('src', ['ACT_EVENT'], type='varchar', alias='act_event') }},
        {{ trx_json_extract('src', ['ACT_ACT'], type='float', alias='act_act') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5firstact') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['act_event']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['act_lastsaved']) }}
