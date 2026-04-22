{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5IESINTERESTCENTERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ENI_LASTSAVED'], type='timestamp_ntz', alias='eni_lastsaved') }},
        {{ trx_json_extract('src', ['ENI_DESC'], type='varchar', alias='eni_desc') }},
        {{ trx_json_extract('src', ['ENI_NOTUSED'], type='varchar', alias='eni_notused') }},
        {{ trx_json_extract('src', ['ENI_UPDATECOUNT'], type='float', alias='eni_updatecount') }},
        {{ trx_json_extract('src', ['ENI_CODE'], type='varchar', alias='eni_code') }},
        {{ trx_json_extract('src', ['ENI_CATEGORY'], type='varchar', alias='eni_category') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5iesinterestcenters') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['eni_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['eni_lastsaved']) }}
