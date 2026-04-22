{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILCONDITIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAC_LASTSAVED'], type='timestamp_ntz', alias='mac_lastsaved') }},
        {{ trx_json_extract('src', ['MAC_TEMPLATE'], type='varchar', alias='mac_template') }},
        {{ trx_json_extract('src', ['MAC_COLUMN'], type='varchar', alias='mac_column') }},
        {{ trx_json_extract('src', ['MAC_CRITERIA'], type='varchar', alias='mac_criteria') }},
        {{ trx_json_extract('src', ['MAC_VALUE1'], type='varchar', alias='mac_value1') }},
        {{ trx_json_extract('src', ['MAC_UPDATECOUNT'], type='float', alias='mac_updatecount') }},
        {{ trx_json_extract('src', ['MAC_COLUMNGRO'], type='float', alias='mac_columngro') }},
        {{ trx_json_extract('src', ['MAC_ANDOR'], type='varchar', alias='mac_andor') }},
        {{ trx_json_extract('src', ['MAC_ATTRIBPK'], type='varchar', alias='mac_attribpk') }},
        {{ trx_json_extract('src', ['MAC_PK'], type='varchar', alias='mac_pk') }},
        {{ trx_json_extract('src', ['MAC_TABLE'], type='varchar', alias='mac_table') }},
        {{ trx_json_extract('src', ['MAC_VALUE2'], type='varchar', alias='mac_value2') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailconditions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mac_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mac_lastsaved']) }}
