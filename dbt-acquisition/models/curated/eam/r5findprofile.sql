{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FINDPROFILE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FPF_LASTSAVED'], type='timestamp_ntz', alias='fpf_lastsaved') }},
        {{ trx_json_extract('src', ['FPF_PROFILE_ORG'], type='varchar', alias='fpf_profile_org') }},
        {{ trx_json_extract('src', ['FPF_CODE'], type='varchar', alias='fpf_code') }},
        {{ trx_json_extract('src', ['FPF_PROFILE'], type='varchar', alias='fpf_profile') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5findprofile') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fpf_code', 'fpf_profile_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fpf_lastsaved']) }}
