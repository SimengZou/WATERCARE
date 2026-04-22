{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWOCCUPATIONTYPES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZOT_LASTSAVED'], type='timestamp_ntz', alias='zot_lastsaved') }},
        {{ trx_json_extract('src', ['ZOT_RDESC'], type='varchar', alias='zot_rdesc') }},
        {{ trx_json_extract('src', ['ZOT_DESC'], type='varchar', alias='zot_desc') }},
        {{ trx_json_extract('src', ['ZOT_RCODE'], type='varchar', alias='zot_rcode') }},
        {{ trx_json_extract('src', ['ZOT_KEY'], type='float', alias='zot_key') }},
        {{ trx_json_extract('src', ['ZOT_CODE'], type='varchar', alias='zot_code') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwoccupationtypes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zot_lastsaved']) }}
