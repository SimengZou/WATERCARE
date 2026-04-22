{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5INSTALL',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['INS_LASTSAVED'], type='timestamp_ntz', alias='ins_lastsaved') }},
        {{ trx_json_extract('src', ['INS_DESC'], type='varchar', alias='ins_desc') }},
        {{ trx_json_extract('src', ['INS_COMMENT'], type='varchar', alias='ins_comment') }},
        {{ trx_json_extract('src', ['INS_FIXED'], type='varchar', alias='ins_fixed') }},
        {{ trx_json_extract('src', ['INS_UPDATECOUNT'], type='float', alias='ins_updatecount') }},
        {{ trx_json_extract('src', ['INS_EXTENDED'], type='varchar', alias='ins_extended') }},
        {{ trx_json_extract('src', ['INS_EDCOMMENT'], type='varchar', alias='ins_edcomment') }},
        {{ trx_json_extract('src', ['INS_CODE'], type='varchar', alias='ins_code') }},
        {{ trx_json_extract('src', ['INS_MODULE'], type='varchar', alias='ins_module') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5install') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ins_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ins_lastsaved']) }}
