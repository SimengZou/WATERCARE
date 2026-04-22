{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USEROBJECTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UOB_LASTSAVED'], type='timestamp_ntz', alias='uob_lastsaved') }},
        {{ trx_json_extract('src', ['UOB_OBJECTTYPE'], type='varchar', alias='uob_objecttype') }},
        {{ trx_json_extract('src', ['UOB_OBJ_XTYPE'], type='varchar', alias='uob_obj_xtype') }},
        {{ trx_json_extract('src', ['UOB_DATABASE'], type='varchar', alias='uob_database') }},
        {{ trx_json_extract('src', ['UOB_OBJECTNAME'], type='varchar', alias='uob_objectname') }},
        {{ trx_json_extract('src', ['UOB_OBJECTSUBTYPE'], type='varchar', alias='uob_objectsubtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userobjects') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['uob_lastsaved']) }}
