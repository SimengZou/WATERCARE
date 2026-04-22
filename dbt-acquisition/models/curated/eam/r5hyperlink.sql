{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HYPERLINK',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HYP_LASTSAVED'], type='timestamp_ntz', alias='hyp_lastsaved') }},
        {{ trx_json_extract('src', ['HYP_SOURCEELEMENTID'], type='varchar', alias='hyp_sourceelementid') }},
        {{ trx_json_extract('src', ['HYP_DESTPAGENAME'], type='varchar', alias='hyp_destpagename') }},
        {{ trx_json_extract('src', ['HYP_DESTELEMENTID'], type='varchar', alias='hyp_destelementid') }},
        {{ trx_json_extract('src', ['HYP_SCREENMODE'], type='varchar', alias='hyp_screenmode') }},
        {{ trx_json_extract('src', ['HYP_PERFORMEXACTQUERY'], type='varchar', alias='hyp_performexactquery') }},
        {{ trx_json_extract('src', ['HYP_UPDATECOUNT'], type='float', alias='hyp_updatecount') }},
        {{ trx_json_extract('src', ['HYP_SRCLINENUMBER'], type='float', alias='hyp_srclinenumber') }},
        {{ trx_json_extract('src', ['HYP_LINKNAME'], type='varchar', alias='hyp_linkname') }},
        {{ trx_json_extract('src', ['HYP_PK'], type='float', alias='hyp_pk') }},
        {{ trx_json_extract('src', ['HYP_SOURCEPAGENAME'], type='varchar', alias='hyp_sourcepagename') }},
        {{ trx_json_extract('src', ['HYP_DATASPY'], type='float', alias='hyp_dataspy') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5hyperlink') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hyp_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hyp_lastsaved']) }}
