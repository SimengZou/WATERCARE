{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5CIWORESULTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['CIR_WORKORDERNUM'], type='varchar', alias='cir_workordernum') }},
        {{ trx_json_extract('src', ['CIR_TYPE'], type='varchar', alias='cir_type') }},
        {{ trx_json_extract('src', ['CIR_CODE'], type='varchar', alias='cir_code') }},
        {{ trx_json_extract('src', ['CIR_DETAIL'], type='varchar', alias='cir_detail') }},
        {{ trx_json_extract('src', ['CIR_OBSVALUE'], type='varchar', alias='cir_obsvalue') }},
        {{ trx_json_extract('src', ['CIR_OBSDATE'], type='timestamp_ntz', alias='cir_obsdate') }},
        {{ trx_json_extract('src', ['CIR_OBSDESCRIPTION'], type='varchar', alias='cir_obsdescription') }},
        {{ trx_json_extract('src', ['CIR_OBSUOM'], type='varchar', alias='cir_obsuom') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['CIR_SEQUENCE'], type='float', alias='cir_sequence') }},
        {{ trx_json_extract('src', ['CIR_TRANSID'], type='varchar', alias='cir_transid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5ciworesults') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cir_sequence']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
