{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DUXLAYOUT',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DXL_LASTSAVED'], type='timestamp_ntz', alias='dxl_lastsaved') }},
        {{ trx_json_extract('src', ['DXL_ELEMENTID'], type='varchar', alias='dxl_elementid') }},
        {{ trx_json_extract('src', ['DXL_ATTRIBUTE'], type='varchar', alias='dxl_attribute') }},
        {{ trx_json_extract('src', ['DXL_SYSTEMATTRIBUTE'], type='varchar', alias='dxl_systemattribute') }},
        {{ trx_json_extract('src', ['DXL_PRESENTINJSP'], type='varchar', alias='dxl_presentinjsp') }},
        {{ trx_json_extract('src', ['DXL_SECTION'], type='varchar', alias='dxl_section') }},
        {{ trx_json_extract('src', ['DXL_SECTIONPOSITION'], type='float', alias='dxl_sectionposition') }},
        {{ trx_json_extract('src', ['DXL_POSITIONINGROUP'], type='float', alias='dxl_positioningroup') }},
        {{ trx_json_extract('src', ['DXL_SECTIONLABEL'], type='varchar', alias='dxl_sectionlabel') }},
        {{ trx_json_extract('src', ['DXL_NEWCARD'], type='varchar', alias='dxl_newcard') }},
        {{ trx_json_extract('src', ['DXL_FIELDINFO'], type='varchar', alias='dxl_fieldinfo') }},
        {{ trx_json_extract('src', ['DXL_DEFAULTVALUE'], type='varchar', alias='dxl_defaultvalue') }},
        {{ trx_json_extract('src', ['DXL_RADIOOPTIONS'], type='varchar', alias='dxl_radiooptions') }},
        {{ trx_json_extract('src', ['DXL_UPDATECOUNT'], type='float', alias='dxl_updatecount') }},
        {{ trx_json_extract('src', ['DXL_USERGROUP'], type='varchar', alias='dxl_usergroup') }},
        {{ trx_json_extract('src', ['DXL_PAGENAME'], type='varchar', alias='dxl_pagename') }},
        {{ trx_json_extract('src', ['DXL_ELEMENTTYPE'], type='varchar', alias='dxl_elementtype') }},
        {{ trx_json_extract('src', ['DXL_FIELDSIZE'], type='float', alias='dxl_fieldsize') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5duxlayout') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dxl_usergroup', 'dxl_pagename', 'dxl_elementid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dxl_lastsaved']) }}
