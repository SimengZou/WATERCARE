{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUNCTIONTABS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FTB_LASTSAVED'], type='timestamp_ntz', alias='ftb_lastsaved') }},
        {{ trx_json_extract('src', ['FTB_PRODUCT'], type='varchar', alias='ftb_product') }},
        {{ trx_json_extract('src', ['FTB_RENTITY'], type='varchar', alias='ftb_rentity') }},
        {{ trx_json_extract('src', ['FTB_VISIBLE'], type='varchar', alias='ftb_visible') }},
        {{ trx_json_extract('src', ['FTB_SELECT'], type='varchar', alias='ftb_select') }},
        {{ trx_json_extract('src', ['FTB_UPDATE'], type='varchar', alias='ftb_update') }},
        {{ trx_json_extract('src', ['FTB_INSERT'], type='varchar', alias='ftb_insert') }},
        {{ trx_json_extract('src', ['FTB_DELETE'], type='varchar', alias='ftb_delete') }},
        {{ trx_json_extract('src', ['FTB_DISPLAYFT'], type='varchar', alias='ftb_displayft') }},
        {{ trx_json_extract('src', ['FTB_SYSREQUIRED'], type='varchar', alias='ftb_sysrequired') }},
        {{ trx_json_extract('src', ['FTB_SEQ'], type='float', alias='ftb_seq') }},
        {{ trx_json_extract('src', ['FTB_SQLEXIST'], type='varchar', alias='ftb_sqlexist') }},
        {{ trx_json_extract('src', ['FTB_UPDATECOUNT'], type='float', alias='ftb_updatecount') }},
        {{ trx_json_extract('src', ['FTB_ALTERSAVE'], type='varchar', alias='ftb_altersave') }},
        {{ trx_json_extract('src', ['FTB_INTERFACECODE'], type='varchar', alias='ftb_interfacecode') }},
        {{ trx_json_extract('src', ['FTB_MEKEY'], type='varchar', alias='ftb_mekey') }},
        {{ trx_json_extract('src', ['FTB_EWSBTN'], type='varchar', alias='ftb_ewsbtn') }},
        {{ trx_json_extract('src', ['FTB_XTYPE'], type='varchar', alias='ftb_xtype') }},
        {{ trx_json_extract('src', ['FTB_NODESIGN'], type='varchar', alias='ftb_nodesign') }},
        {{ trx_json_extract('src', ['FTB_DESIGNPOPUP'], type='varchar', alias='ftb_designpopup') }},
        {{ trx_json_extract('src', ['FTB_FUNCTION'], type='varchar', alias='ftb_function') }},
        {{ trx_json_extract('src', ['FTB_TAB'], type='varchar', alias='ftb_tab') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5functiontabs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ftb_function', 'ftb_tab']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ftb_lastsaved']) }}
