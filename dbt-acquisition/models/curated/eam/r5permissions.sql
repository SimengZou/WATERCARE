{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PERMISSIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PRM_LASTSAVED'], type='timestamp_ntz', alias='prm_lastsaved') }},
        {{ trx_json_extract('src', ['PRM_SELECT'], type='varchar', alias='prm_select') }},
        {{ trx_json_extract('src', ['PRM_UPDATE'], type='varchar', alias='prm_update') }},
        {{ trx_json_extract('src', ['PRM_INSERT'], type='varchar', alias='prm_insert') }},
        {{ trx_json_extract('src', ['PRM_DELETE'], type='varchar', alias='prm_delete') }},
        {{ trx_json_extract('src', ['PRM_PRINTER'], type='varchar', alias='prm_printer') }},
        {{ trx_json_extract('src', ['PRM_SCREEN'], type='varchar', alias='prm_screen') }},
        {{ trx_json_extract('src', ['PRM_PRFILE'], type='varchar', alias='prm_prfile') }},
        {{ trx_json_extract('src', ['PRM_LOCAL'], type='varchar', alias='prm_local') }},
        {{ trx_json_extract('src', ['PRM_DEFQUERY'], type='varchar', alias='prm_defquery') }},
        {{ trx_json_extract('src', ['PRM_OVERRULE'], type='varchar', alias='prm_overrule') }},
        {{ trx_json_extract('src', ['PRM_INPUT'], type='varchar', alias='prm_input') }},
        {{ trx_json_extract('src', ['PRM_UPDATECOUNT'], type='float', alias='prm_updatecount') }},
        {{ trx_json_extract('src', ['PRM_CREATED'], type='timestamp_ntz', alias='prm_created') }},
        {{ trx_json_extract('src', ['PRM_UPDATED'], type='timestamp_ntz', alias='prm_updated') }},
        {{ trx_json_extract('src', ['PRM_SECURITYDDSPYID'], type='float', alias='prm_securityddspyid') }},
        {{ trx_json_extract('src', ['PRM_FUNCTION'], type='varchar', alias='prm_function') }},
        {{ trx_json_extract('src', ['PRM_GROUP'], type='varchar', alias='prm_group') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5permissions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['prm_function', 'prm_group']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['prm_lastsaved']) }}
