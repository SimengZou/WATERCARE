{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PAGELAYOUT_BAK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PLO_LASTSAVED'], type='timestamp_ntz', alias='plo_lastsaved') }},
        {{ trx_json_extract('src', ['PLO_USERGROUP'], type='varchar', alias='plo_usergroup') }},
        {{ trx_json_extract('src', ['PLO_ELEMENTTYPE'], type='varchar', alias='plo_elementtype') }},
        {{ trx_json_extract('src', ['PLO_PRESENTINJSP'], type='varchar', alias='plo_presentinjsp') }},
        {{ trx_json_extract('src', ['PLO_ATTRIBUTE'], type='varchar', alias='plo_attribute') }},
        {{ trx_json_extract('src', ['PLO_FIELDCONTAINER'], type='varchar', alias='plo_fieldcontainer') }},
        {{ trx_json_extract('src', ['PLO_FIELDGROUP'], type='float', alias='plo_fieldgroup') }},
        {{ trx_json_extract('src', ['PLO_POSITIONINGROUP'], type='float', alias='plo_positioningroup') }},
        {{ trx_json_extract('src', ['PLO_FIELDCONTTYPE'], type='float', alias='plo_fieldconttype') }},
        {{ trx_json_extract('src', ['PLO_TABINDEX'], type='float', alias='plo_tabindex') }},
        {{ trx_json_extract('src', ['PLO_SYSTEMATTRIBUTE'], type='varchar', alias='plo_systemattribute') }},
        {{ trx_json_extract('src', ['PLO_CHANGED'], type='varchar', alias='plo_changed') }},
        {{ trx_json_extract('src', ['PLO_UPDATECOUNT'], type='float', alias='plo_updatecount') }},
        {{ trx_json_extract('src', ['PLO_MEKEY'], type='varchar', alias='plo_mekey') }},
        {{ trx_json_extract('src', ['PLO_MEFLAG'], type='varchar', alias='plo_meflag') }},
        {{ trx_json_extract('src', ['PLO_DEFAULTVALUE'], type='varchar', alias='plo_defaultvalue') }},
        {{ trx_json_extract('src', ['PLO_PARENTPAGE'], type='varchar', alias='plo_parentpage') }},
        {{ trx_json_extract('src', ['PLO_PAGENAME'], type='varchar', alias='plo_pagename') }},
        {{ trx_json_extract('src', ['PLO_ELEMENTID'], type='varchar', alias='plo_elementid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pagelayout_bak') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['plo_lastsaved']) }}
