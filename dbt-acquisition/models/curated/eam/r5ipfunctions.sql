{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IPFUNCTIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['IPF_LASTSAVED'], type='timestamp_ntz', alias='ipf_lastsaved') }},
        {{ trx_json_extract('src', ['IPF_DESC'], type='varchar', alias='ipf_desc') }},
        {{ trx_json_extract('src', ['IPF_UPDATECOUNT'], type='float', alias='ipf_updatecount') }},
        {{ trx_json_extract('src', ['IPF_CODE'], type='float', alias='ipf_code') }},
        {{ trx_json_extract('src', ['IPF_FIELD'], type='varchar', alias='ipf_field') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5ipfunctions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ipf_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ipf_lastsaved']) }}
