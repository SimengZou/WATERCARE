{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5GRIDFIELD',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['GFD_LASTSAVED'], type='timestamp_ntz', alias='gfd_lastsaved') }},
        {{ trx_json_extract('src', ['GFD_BOTFUNCTION'], type='varchar', alias='gfd_botfunction') }},
        {{ trx_json_extract('src', ['GFD_BOTNUMBER'], type='float', alias='gfd_botnumber') }},
        {{ trx_json_extract('src', ['GFD_CONTROLTYPE'], type='varchar', alias='gfd_controltype') }},
        {{ trx_json_extract('src', ['GFD_SCRIPTEVENT'], type='varchar', alias='gfd_scriptevent') }},
        {{ trx_json_extract('src', ['GFD_TAGNAME'], type='varchar', alias='gfd_tagname') }},
        {{ trx_json_extract('src', ['GFD_UPDATECOUNT'], type='float', alias='gfd_updatecount') }},
        {{ trx_json_extract('src', ['GFD_TAGPARAMS'], type='varchar', alias='gfd_tagparams') }},
        {{ trx_json_extract('src', ['GFD_AGGFUNC'], type='varchar', alias='gfd_aggfunc') }},
        {{ trx_json_extract('src', ['GFD_FIELDTYPE'], type='varchar', alias='gfd_fieldtype') }},
        {{ trx_json_extract('src', ['GFD_OCCURRENCE'], type='float', alias='gfd_occurrence') }},
        {{ trx_json_extract('src', ['GFD_DEPENDENT'], type='float', alias='gfd_dependent') }},
        {{ trx_json_extract('src', ['GFD_SECENTITY'], type='varchar', alias='gfd_secentity') }},
        {{ trx_json_extract('src', ['GFD_HEADERLOCATION'], type='varchar', alias='gfd_headerlocation') }},
        {{ trx_json_extract('src', ['GFD_UPDATED'], type='timestamp_ntz', alias='gfd_updated') }},
        {{ trx_json_extract('src', ['GFD_FIELDID'], type='float', alias='gfd_fieldid') }},
        {{ trx_json_extract('src', ['GFD_GRIDID'], type='float', alias='gfd_gridid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5gridfield') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['gfd_fieldid', 'gfd_gridid', 'gfd_occurrence']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['gfd_lastsaved']) }}
