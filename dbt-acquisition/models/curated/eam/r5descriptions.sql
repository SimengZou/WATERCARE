{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DESCRIPTIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DES_LASTSAVED'], type='timestamp_ntz', alias='des_lastsaved') }},
        {{ trx_json_extract('src', ['DES_RENTITY'], type='varchar', alias='des_rentity') }},
        {{ trx_json_extract('src', ['DES_TYPE'], type='varchar', alias='des_type') }},
        {{ trx_json_extract('src', ['DES_RTYPE'], type='varchar', alias='des_rtype') }},
        {{ trx_json_extract('src', ['DES_CODE'], type='varchar', alias='des_code') }},
        {{ trx_json_extract('src', ['DES_TEXT'], type='varchar', alias='des_text') }},
        {{ trx_json_extract('src', ['DES_TRANS'], type='varchar', alias='des_trans') }},
        {{ trx_json_extract('src', ['DES_ORG'], type='varchar', alias='des_org') }},
        {{ trx_json_extract('src', ['DES_UPDATECOUNT'], type='float', alias='des_updatecount') }},
        {{ trx_json_extract('src', ['DES_ENTITY'], type='varchar', alias='des_entity') }},
        {{ trx_json_extract('src', ['DES_LANG'], type='varchar', alias='des_lang') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5descriptions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['des_rentity', 'des_code', 'des_org', 'des_lang', 'des_rtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['des_lastsaved']) }}
