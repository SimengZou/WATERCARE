{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUTUREEVENTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FUT_LASTSAVED'], type='timestamp_ntz', alias='fut_lastsaved') }},
        {{ trx_json_extract('src', ['FUT_SEQNO'], type='float', alias='fut_seqno') }},
        {{ trx_json_extract('src', ['FUT_UPDATECOUNT'], type='float', alias='fut_updatecount') }},
        {{ trx_json_extract('src', ['FUT_MP_SEQ'], type='float', alias='fut_mp_seq') }},
        {{ trx_json_extract('src', ['FUT_EVENT'], type='varchar', alias='fut_event') }},
        {{ trx_json_extract('src', ['FUT_DATE'], type='timestamp_ntz', alias='fut_date') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5futureevents') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fut_event', 'fut_seqno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fut_lastsaved']) }}
