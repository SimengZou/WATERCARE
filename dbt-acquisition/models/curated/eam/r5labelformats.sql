{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LABELFORMATS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LBL_LASTSAVED'], type='timestamp_ntz', alias='lbl_lastsaved') }},
        {{ trx_json_extract('src', ['LBL_FILENAME'], type='varchar', alias='lbl_filename') }},
        {{ trx_json_extract('src', ['LBL_DESC'], type='varchar', alias='lbl_desc') }},
        {{ trx_json_extract('src', ['LBL_PRINTERTYPE'], type='varchar', alias='lbl_printertype') }},
        {{ trx_json_extract('src', ['LBL_CLASS'], type='varchar', alias='lbl_class') }},
        {{ trx_json_extract('src', ['LBL_CLASS_ORG'], type='varchar', alias='lbl_class_org') }},
        {{ trx_json_extract('src', ['LBL_UPDATECOUNT'], type='float', alias='lbl_updatecount') }},
        {{ trx_json_extract('src', ['LBL_CODE'], type='varchar', alias='lbl_code') }},
        {{ trx_json_extract('src', ['LBL_FIELDS'], type='varchar', alias='lbl_fields') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5labelformats') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['lbl_code', 'lbl_class', 'lbl_class_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lbl_lastsaved']) }}
