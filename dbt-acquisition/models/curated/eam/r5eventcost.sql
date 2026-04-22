{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5EVENTCOST',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EVO_LASTSAVED'], type='timestamp_ntz', alias='evo_lastsaved') }},
        {{ trx_json_extract('src', ['EVO_DIRECTMATERIAL'], type='float', alias='evo_directmaterial') }},
        {{ trx_json_extract('src', ['EVO_COSTCALCULATED'], type='varchar', alias='evo_costcalculated') }},
        {{ trx_json_extract('src', ['EVO_LABOR'], type='float', alias='evo_labor') }},
        {{ trx_json_extract('src', ['EVO_MATERIAL'], type='float', alias='evo_material') }},
        {{ trx_json_extract('src', ['EVO_TOOL'], type='float', alias='evo_tool') }},
        {{ trx_json_extract('src', ['EVO_TOTAL'], type='float', alias='evo_total') }},
        {{ trx_json_extract('src', ['EVO_HOURS'], type='float', alias='evo_hours') }},
        {{ trx_json_extract('src', ['EVO_UPDATED'], type='timestamp_ntz', alias='evo_updated') }},
        {{ trx_json_extract('src', ['EVO_OWNLABOR'], type='float', alias='evo_ownlabor') }},
        {{ trx_json_extract('src', ['EVO_HIREDLABOR'], type='float', alias='evo_hiredlabor') }},
        {{ trx_json_extract('src', ['EVO_SERVICESLABOR'], type='float', alias='evo_serviceslabor') }},
        {{ trx_json_extract('src', ['EVO_STOCKMATERIAL'], type='float', alias='evo_stockmaterial') }},
        {{ trx_json_extract('src', ['EVO_EVENT'], type='varchar', alias='evo_event') }},
        {{ trx_json_extract('src', ['EVO_RECALCCOST'], type='varchar', alias='evo_recalccost') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5eventcost') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['evo_event']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['evo_lastsaved']) }}
