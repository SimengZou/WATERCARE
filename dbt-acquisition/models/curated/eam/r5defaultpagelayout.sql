{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DEFAULTPAGELAYOUT',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PLD_LASTSAVED'], type='timestamp_ntz', alias='pld_lastsaved') }},
        {{ trx_json_extract('src', ['PLD_RESPONSEXPATH'], type='varchar', alias='pld_responsexpath') }},
        {{ trx_json_extract('src', ['PLD_ELEMENTTYPE'], type='varchar', alias='pld_elementtype') }},
        {{ trx_json_extract('src', ['PLD_PRESENTINJSP'], type='varchar', alias='pld_presentinjsp') }},
        {{ trx_json_extract('src', ['PLD_ATTRIBUTE'], type='varchar', alias='pld_attribute') }},
        {{ trx_json_extract('src', ['PLD_FIELDCONTAINER'], type='varchar', alias='pld_fieldcontainer') }},
        {{ trx_json_extract('src', ['PLD_FIELDGROUP'], type='float', alias='pld_fieldgroup') }},
        {{ trx_json_extract('src', ['PLD_POSITIONINGROUP'], type='float', alias='pld_positioningroup') }},
        {{ trx_json_extract('src', ['PLD_FIELDCONTTYPE'], type='float', alias='pld_fieldconttype') }},
        {{ trx_json_extract('src', ['PLD_TABINDEX'], type='float', alias='pld_tabindex') }},
        {{ trx_json_extract('src', ['PLD_SYSTEMATTRIBUTE'], type='varchar', alias='pld_systemattribute') }},
        {{ trx_json_extract('src', ['PLD_UPDATECOUNT'], type='float', alias='pld_updatecount') }},
        {{ trx_json_extract('src', ['PLD_DDTABLENAME'], type='varchar', alias='pld_ddtablename') }},
        {{ trx_json_extract('src', ['PLD_DDFIELDNAME'], type='varchar', alias='pld_ddfieldname') }},
        {{ trx_json_extract('src', ['PLD_XPATH'], type='varchar', alias='pld_xpath') }},
        {{ trx_json_extract('src', ['PLD_FIELDTYPE'], type='varchar', alias='pld_fieldtype') }},
        {{ trx_json_extract('src', ['PLD_SIZE'], type='float', alias='pld_size') }},
        {{ trx_json_extract('src', ['PLD_MAXLENGTH'], type='float', alias='pld_maxlength') }},
        {{ trx_json_extract('src', ['PLD_CASE'], type='varchar', alias='pld_case') }},
        {{ trx_json_extract('src', ['PLD_OTHERATTRIBS'], type='varchar', alias='pld_otherattribs') }},
        {{ trx_json_extract('src', ['PLD_NUMBERTYPE'], type='varchar', alias='pld_numbertype') }},
        {{ trx_json_extract('src', ['PLD_ONLOOKUP'], type='varchar', alias='pld_onlookup') }},
        {{ trx_json_extract('src', ['PLD_ONVALIDATE'], type='varchar', alias='pld_onvalidate') }},
        {{ trx_json_extract('src', ['PLD_ONCLASSCHANGE'], type='varchar', alias='pld_onclasschange') }},
        {{ trx_json_extract('src', ['PLD_ONCHAINEDLOOKUP'], type='varchar', alias='pld_onchainedlookup') }},
        {{ trx_json_extract('src', ['PLD_JSPFILE'], type='varchar', alias='pld_jspfile') }},
        {{ trx_json_extract('src', ['PLD_OTHERTAGS'], type='varchar', alias='pld_othertags') }},
        {{ trx_json_extract('src', ['PLD_DESTXPATH'], type='varchar', alias='pld_destxpath') }},
        {{ trx_json_extract('src', ['PLD_PAGENAME'], type='varchar', alias='pld_pagename') }},
        {{ trx_json_extract('src', ['PLD_ELEMENTID'], type='varchar', alias='pld_elementid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5defaultpagelayout') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pld_pagename', 'pld_elementid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pld_lastsaved']) }}
