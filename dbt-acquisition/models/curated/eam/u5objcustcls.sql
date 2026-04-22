{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5OBJCUSTCLS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OCC_CODE'], type='varchar', alias='occ_code') }},
        {{ trx_json_extract('src', ['OCC_ORG'], type='varchar', alias='occ_org') }},
        {{ trx_json_extract('src', ['OCC_UDFCHAR12'], type='varchar', alias='occ_udfchar12') }},
        {{ trx_json_extract('src', ['OCC_VALUE'], type='varchar', alias='occ_value') }},
        {{ trx_json_extract('src', ['OCC_LINE'], type='float', alias='occ_line') }},
        {{ trx_json_extract('src', ['OCC_PROPERTY'], type='varchar', alias='occ_property') }},
        {{ trx_json_extract('src', ['OCC_TEXT'], type='varchar', alias='occ_text') }},
        {{ trx_json_extract('src', ['OCC_CLASS'], type='varchar', alias='occ_class') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5objcustcls') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}
