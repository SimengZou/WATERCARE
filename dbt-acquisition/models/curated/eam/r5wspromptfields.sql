{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WSPROMPTFIELDS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WSF_LASTSAVED'], type='timestamp_ntz', alias='wsf_lastsaved') }},
        {{ trx_json_extract('src', ['WSF_RESPONSEXPATH'], type='varchar', alias='wsf_responsexpath') }},
        {{ trx_json_extract('src', ['WSF_LINE'], type='float', alias='wsf_line') }},
        {{ trx_json_extract('src', ['WSF_FIELD'], type='varchar', alias='wsf_field') }},
        {{ trx_json_extract('src', ['WSF_FIELDLABEL'], type='varchar', alias='wsf_fieldlabel') }},
        {{ trx_json_extract('src', ['WSF_NEXTLINE'], type='float', alias='wsf_nextline') }},
        {{ trx_json_extract('src', ['WSF_RTYPE'], type='varchar', alias='wsf_rtype') }},
        {{ trx_json_extract('src', ['WSF_TYPE'], type='varchar', alias='wsf_type') }},
        {{ trx_json_extract('src', ['WSF_MAXLENGTH'], type='float', alias='wsf_maxlength') }},
        {{ trx_json_extract('src', ['WSF_MINLENGTH'], type='float', alias='wsf_minlength') }},
        {{ trx_json_extract('src', ['WSF_DUPPREVVALUE'], type='varchar', alias='wsf_dupprevvalue') }},
        {{ trx_json_extract('src', ['WSF_BRANCHCOND'], type='varchar', alias='wsf_branchcond') }},
        {{ trx_json_extract('src', ['WSF_BRANCHPATTERN'], type='varchar', alias='wsf_branchpattern') }},
        {{ trx_json_extract('src', ['WSF_BRANCHGOTO'], type='float', alias='wsf_branchgoto') }},
        {{ trx_json_extract('src', ['WSF_RETRIEVEFROMGROUP'], type='float', alias='wsf_retrievefromgroup') }},
        {{ trx_json_extract('src', ['WSF_RETRIEVEFIELD'], type='varchar', alias='wsf_retrievefield') }},
        {{ trx_json_extract('src', ['WSF_DESTWEBSERVICE'], type='varchar', alias='wsf_destwebservice') }},
        {{ trx_json_extract('src', ['WSF_RETRIEVEXPATH'], type='varchar', alias='wsf_retrievexpath') }},
        {{ trx_json_extract('src', ['WSF_DESTXPATH'], type='varchar', alias='wsf_destxpath') }},
        {{ trx_json_extract('src', ['WSF_FIELDXPATH'], type='varchar', alias='wsf_fieldxpath') }},
        {{ trx_json_extract('src', ['WSF_FIXEDDATA'], type='varchar', alias='wsf_fixeddata') }},
        {{ trx_json_extract('src', ['WSF_COMPUTEDDATA'], type='varchar', alias='wsf_computeddata') }},
        {{ trx_json_extract('src', ['WSF_PATTERN'], type='varchar', alias='wsf_pattern') }},
        {{ trx_json_extract('src', ['WSF_SQLQUERYCODE'], type='varchar', alias='wsf_sqlquerycode') }},
        {{ trx_json_extract('src', ['WSF_DISPLAYTYPE'], type='varchar', alias='wsf_displaytype') }},
        {{ trx_json_extract('src', ['WSF_UNMAPPED'], type='varchar', alias='wsf_unmapped') }},
        {{ trx_json_extract('src', ['WSF_UPDATECOUNT'], type='float', alias='wsf_updatecount') }},
        {{ trx_json_extract('src', ['WSF_UPDATED'], type='timestamp_ntz', alias='wsf_updated') }},
        {{ trx_json_extract('src', ['WSF_MOBILEQUERYCODE'], type='varchar', alias='wsf_mobilequerycode') }},
        {{ trx_json_extract('src', ['WSF_ISCONTROLORG'], type='varchar', alias='wsf_iscontrolorg') }},
        {{ trx_json_extract('src', ['WSF_ISCLASS'], type='varchar', alias='wsf_isclass') }},
        {{ trx_json_extract('src', ['WSF_ISCLASSORG'], type='varchar', alias='wsf_isclassorg') }},
        {{ trx_json_extract('src', ['WSF_ISCATEGORY'], type='varchar', alias='wsf_iscategory') }},
        {{ trx_json_extract('src', ['WSF_SYSTEMFIELDTYPE'], type='varchar', alias='wsf_systemfieldtype') }},
        {{ trx_json_extract('src', ['WSF_CUSTOMFIELDID'], type='varchar', alias='wsf_customfieldid') }},
        {{ trx_json_extract('src', ['WSF_CLEARPREVVALUES'], type='varchar', alias='wsf_clearprevvalues') }},
        {{ trx_json_extract('src', ['WSF_TAGNAME'], type='varchar', alias='wsf_tagname') }},
        {{ trx_json_extract('src', ['WSF_PRIMARYKEY'], type='varchar', alias='wsf_primarykey') }},
        {{ trx_json_extract('src', ['WSF_PRECISION'], type='float', alias='wsf_precision') }},
        {{ trx_json_extract('src', ['WSF_SCALE'], type='float', alias='wsf_scale') }},
        {{ trx_json_extract('src', ['WSF_UPPERCASE'], type='varchar', alias='wsf_uppercase') }},
        {{ trx_json_extract('src', ['WSF_WSPMTCODE'], type='varchar', alias='wsf_wspmtcode') }},
        {{ trx_json_extract('src', ['WSF_PROCESSGROUP'], type='float', alias='wsf_processgroup') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5wspromptfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wsf_wspmtcode', 'wsf_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wsf_lastsaved']) }}
