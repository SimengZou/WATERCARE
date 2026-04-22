{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SHIFTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SHF_LASTSAVED'], type='timestamp_ntz', alias='shf_lastsaved') }},
        {{ trx_json_extract('src', ['SHF_DESC'], type='varchar', alias='shf_desc') }},
        {{ trx_json_extract('src', ['SHF_CLASS'], type='varchar', alias='shf_class') }},
        {{ trx_json_extract('src', ['SHF_LENGTH'], type='float', alias='shf_length') }},
        {{ trx_json_extract('src', ['SHF_ORG'], type='varchar', alias='shf_org') }},
        {{ trx_json_extract('src', ['SHF_CLASS_ORG'], type='varchar', alias='shf_class_org') }},
        {{ trx_json_extract('src', ['SHF_UPDATECOUNT'], type='float', alias='shf_updatecount') }},
        {{ trx_json_extract('src', ['SHF_UPDATED'], type='timestamp_ntz', alias='shf_updated') }},
        {{ trx_json_extract('src', ['SHF_CODE'], type='varchar', alias='shf_code') }},
        {{ trx_json_extract('src', ['SHF_START'], type='timestamp_ntz', alias='shf_start') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5shifts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['shf_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['shf_lastsaved']) }}
