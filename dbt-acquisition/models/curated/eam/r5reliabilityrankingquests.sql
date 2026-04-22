{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5RELIABILITYRANKINGQUESTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RRQ_LASTSAVED'], type='timestamp_ntz', alias='rrq_lastsaved') }},
        {{ trx_json_extract('src', ['RRQ_LANG'], type='varchar', alias='rrq_lang') }},
        {{ trx_json_extract('src', ['RRQ_TRANS'], type='varchar', alias='rrq_trans') }},
        {{ trx_json_extract('src', ['RRQ_UPDATECOUNT'], type='float', alias='rrq_updatecount') }},
        {{ trx_json_extract('src', ['RRQ_LEVELPK'], type='varchar', alias='rrq_levelpk') }},
        {{ trx_json_extract('src', ['RRQ_QUESTION'], type='varchar', alias='rrq_question') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reliabilityrankingquests') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rrq_levelpk', 'rrq_lang']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rrq_lastsaved']) }}
