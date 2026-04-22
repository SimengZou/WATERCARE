{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FINDINGS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FND_LASTSAVED'], type='timestamp_ntz', alias='fnd_lastsaved') }},
        {{ trx_json_extract('src', ['FND_DESC'], type='varchar', alias='fnd_desc') }},
        {{ trx_json_extract('src', ['FND_CLASS'], type='varchar', alias='fnd_class') }},
        {{ trx_json_extract('src', ['FND_UPDATED'], type='timestamp_ntz', alias='fnd_updated') }},
        {{ trx_json_extract('src', ['FND_CLASS_ORG'], type='varchar', alias='fnd_class_org') }},
        {{ trx_json_extract('src', ['FND_UPDATECOUNT'], type='float', alias='fnd_updatecount') }},
        {{ trx_json_extract('src', ['FND_CREATED'], type='timestamp_ntz', alias='fnd_created') }},
        {{ trx_json_extract('src', ['FND_CODE'], type='varchar', alias='fnd_code') }},
        {{ trx_json_extract('src', ['FND_GEN'], type='varchar', alias='fnd_gen') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5findings') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fnd_lastsaved']) }}
