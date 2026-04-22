{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EDITIONUCODES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EDU_LASTSAVED'], type='timestamp_ntz', alias='edu_lastsaved') }},
        {{ trx_json_extract('src', ['EDU_CODE'], type='varchar', alias='edu_code') }},
        {{ trx_json_extract('src', ['EDU_LANG'], type='varchar', alias='edu_lang') }},
        {{ trx_json_extract('src', ['EDU_RENTITY'], type='varchar', alias='edu_rentity') }},
        {{ trx_json_extract('src', ['EDU_MARKET'], type='varchar', alias='edu_market') }},
        {{ trx_json_extract('src', ['EDU_TEXTCODE'], type='varchar', alias='edu_textcode') }},
        {{ trx_json_extract('src', ['EDU_RCODE'], type='varchar', alias='edu_rcode') }},
        {{ trx_json_extract('src', ['EDU_DESC'], type='varchar', alias='edu_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5editionucodes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['edu_lastsaved']) }}
