{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5GRID',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['GRD_LASTSAVED'], type='timestamp_ntz', alias='grd_lastsaved') }},
        {{ trx_json_extract('src', ['GRD_MAXGRIDCOST'], type='float', alias='grd_maxgridcost') }},
        {{ trx_json_extract('src', ['GRD_BASEQUERY'], type='varchar', alias='grd_basequery') }},
        {{ trx_json_extract('src', ['GRD_GRIDNAME'], type='varchar', alias='grd_gridname') }},
        {{ trx_json_extract('src', ['GRD_KEYFIELDS'], type='varchar', alias='grd_keyfields') }},
        {{ trx_json_extract('src', ['GRD_FILTERABLELIST'], type='varchar', alias='grd_filterablelist') }},
        {{ trx_json_extract('src', ['GRD_SORTABLELIST'], type='varchar', alias='grd_sortablelist') }},
        {{ trx_json_extract('src', ['GRD_DISPLAYABLELIST'], type='varchar', alias='grd_displayablelist') }},
        {{ trx_json_extract('src', ['GRD_ORGCOLNAME'], type='varchar', alias='grd_orgcolname') }},
        {{ trx_json_extract('src', ['GRD_BASEQUERY_MULTIORG'], type='varchar', alias='grd_basequery_multiorg') }},
        {{ trx_json_extract('src', ['GRD_KEYFIELDS_MULTIORG'], type='varchar', alias='grd_keyfields_multiorg') }},
        {{ trx_json_extract('src', ['GRD_FILTERABLE_MULTIORG'], type='varchar', alias='grd_filterable_multiorg') }},
        {{ trx_json_extract('src', ['GRD_SORTABLE_MULTIORG'], type='varchar', alias='grd_sortable_multiorg') }},
        {{ trx_json_extract('src', ['GRD_DISPLAYABLE_MULTIORG'], type='varchar', alias='grd_displayable_multiorg') }},
        {{ trx_json_extract('src', ['GRD_BOTFUNCTION'], type='varchar', alias='grd_botfunction') }},
        {{ trx_json_extract('src', ['GRD_UPDATECOUNT'], type='float', alias='grd_updatecount') }},
        {{ trx_json_extract('src', ['GRD_PORTLETFLAG'], type='varchar', alias='grd_portletflag') }},
        {{ trx_json_extract('src', ['GRD_SECENTITY'], type='varchar', alias='grd_secentity') }},
        {{ trx_json_extract('src', ['GRD_HINTS'], type='varchar', alias='grd_hints') }},
        {{ trx_json_extract('src', ['GRD_TAB'], type='varchar', alias='grd_tab') }},
        {{ trx_json_extract('src', ['GRD_GRIDTYPE'], type='float', alias='grd_gridtype') }},
        {{ trx_json_extract('src', ['GRD_UNITS'], type='varchar', alias='grd_units') }},
        {{ trx_json_extract('src', ['GRD_OPTIMIZERON'], type='varchar', alias='grd_optimizeron') }},
        {{ trx_json_extract('src', ['GRD_DISTINCT'], type='varchar', alias='grd_distinct') }},
        {{ trx_json_extract('src', ['GRD_COMMITFLAG'], type='varchar', alias='grd_commitflag') }},
        {{ trx_json_extract('src', ['GRD_CUSTOMFIELDCODE'], type='varchar', alias='grd_customfieldcode') }},
        {{ trx_json_extract('src', ['GRD_COMPLEX'], type='varchar', alias='grd_complex') }},
        {{ trx_json_extract('src', ['GRD_ACTIVE'], type='varchar', alias='grd_active') }},
        {{ trx_json_extract('src', ['GRD_NOSCREENDESIGNER'], type='varchar', alias='grd_noscreendesigner') }},
        {{ trx_json_extract('src', ['GRD_MOBILE'], type='varchar', alias='grd_mobile') }},
        {{ trx_json_extract('src', ['GRD_UPDATED'], type='timestamp_ntz', alias='grd_updated') }},
        {{ trx_json_extract('src', ['GRD_GISDATAFILTER'], type='varchar', alias='grd_gisdatafilter') }},
        {{ trx_json_extract('src', ['GRD_GISWOFIELDMAP'], type='varchar', alias='grd_giswofieldmap') }},
        {{ trx_json_extract('src', ['GRD_DESC'], type='varchar', alias='grd_desc') }},
        {{ trx_json_extract('src', ['GRD_GRIDID'], type='float', alias='grd_gridid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5grid') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['grd_gridid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['grd_lastsaved']) }}
