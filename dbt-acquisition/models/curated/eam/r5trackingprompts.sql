{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRACKINGPROMPTS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TKP_LASTSAVED'], type='timestamp_ntz', alias='tkp_lastsaved') }},
        {{ trx_json_extract('src', ['TKP_PROMPT'], type='varchar', alias='tkp_prompt') }},
        {{ trx_json_extract('src', ['TKP_DATARTYPE'], type='varchar', alias='tkp_datartype') }},
        {{ trx_json_extract('src', ['TKP_FIXEDDATA'], type='varchar', alias='tkp_fixeddata') }},
        {{ trx_json_extract('src', ['TKP_COMPUTEDATA'], type='varchar', alias='tkp_computedata') }},
        {{ trx_json_extract('src', ['TKP_UPLOADCOLUMN'], type='varchar', alias='tkp_uploadcolumn') }},
        {{ trx_json_extract('src', ['TKP_INTERFACERTYPE'], type='varchar', alias='tkp_interfacertype') }},
        {{ trx_json_extract('src', ['TKP_OBTRANSTYPE'], type='varchar', alias='tkp_obtranstype') }},
        {{ trx_json_extract('src', ['TKP_TRANSGROUP'], type='float', alias='tkp_transgroup') }},
        {{ trx_json_extract('src', ['TKP_PROMPTSEQ'], type='float', alias='tkp_promptseq') }},
        {{ trx_json_extract('src', ['TKP_MINSIZE'], type='float', alias='tkp_minsize') }},
        {{ trx_json_extract('src', ['TKP_MAXSIZE'], type='float', alias='tkp_maxsize') }},
        {{ trx_json_extract('src', ['TKP_MATCHPATTERN'], type='varchar', alias='tkp_matchpattern') }},
        {{ trx_json_extract('src', ['TKP_BRANCHCONDITION'], type='varchar', alias='tkp_branchcondition') }},
        {{ trx_json_extract('src', ['TKP_BRANCHPATTERN'], type='varchar', alias='tkp_branchpattern') }},
        {{ trx_json_extract('src', ['TKP_BRANCHGOTO'], type='float', alias='tkp_branchgoto') }},
        {{ trx_json_extract('src', ['TKP_DEFAULTPREVDATA'], type='varchar', alias='tkp_defaultprevdata') }},
        {{ trx_json_extract('src', ['TKP_RETURNTOPROMPT'], type='float', alias='tkp_returntoprompt') }},
        {{ trx_json_extract('src', ['TKP_VALIDATEFILE'], type='varchar', alias='tkp_validatefile') }},
        {{ trx_json_extract('src', ['TKP_OBTRANSRTYPE'], type='varchar', alias='tkp_obtransrtype') }},
        {{ trx_json_extract('src', ['TKP_INTERFACETYPE'], type='varchar', alias='tkp_interfacetype') }},
        {{ trx_json_extract('src', ['TKP_DATATYPE'], type='varchar', alias='tkp_datatype') }},
        {{ trx_json_extract('src', ['TKP_COLUMN'], type='varchar', alias='tkp_column') }},
        {{ trx_json_extract('src', ['TKP_RCOLUMN'], type='varchar', alias='tkp_rcolumn') }},
        {{ trx_json_extract('src', ['TKP_VALIDATELOV'], type='varchar', alias='tkp_validatelov') }},
        {{ trx_json_extract('src', ['TKP_ARCHIVECOLUMN'], type='varchar', alias='tkp_archivecolumn') }},
        {{ trx_json_extract('src', ['TKP_ARCCOLUMN'], type='varchar', alias='tkp_arccolumn') }},
        {{ trx_json_extract('src', ['TKP_ARCRCOLUMN'], type='varchar', alias='tkp_arcrcolumn') }},
        {{ trx_json_extract('src', ['TKP_NOTONFILEFLAG'], type='varchar', alias='tkp_notonfileflag') }},
        {{ trx_json_extract('src', ['TKP_LOVOVERRIDEFLAG'], type='varchar', alias='tkp_lovoverrideflag') }},
        {{ trx_json_extract('src', ['TKP_SQLCODE'], type='varchar', alias='tkp_sqlcode') }},
        {{ trx_json_extract('src', ['TKP_DATEFORMAT'], type='varchar', alias='tkp_dateformat') }},
        {{ trx_json_extract('src', ['TKP_UPDATECOUNT'], type='float', alias='tkp_updatecount') }},
        {{ trx_json_extract('src', ['TKP_UPDATED'], type='timestamp_ntz', alias='tkp_updated') }},
        {{ trx_json_extract('src', ['TKP_LOVID'], type='varchar', alias='tkp_lovid') }},
        {{ trx_json_extract('src', ['TKP_EWSQUERYCODE'], type='varchar', alias='tkp_ewsquerycode') }},
        {{ trx_json_extract('src', ['TKP_TRANS'], type='varchar', alias='tkp_trans') }},
        {{ trx_json_extract('src', ['TKP_TRANSSEQ'], type='float', alias='tkp_transseq') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5trackingprompts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tkp_trans', 'tkp_transseq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tkp_lastsaved']) }}
