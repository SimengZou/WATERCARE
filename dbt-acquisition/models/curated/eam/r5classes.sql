{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CLASSES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CLS_LASTSAVED'], type='timestamp_ntz', alias='cls_lastsaved') }},
        {{ trx_json_extract('src', ['CLS_IMAGE'], type='varchar', alias='cls_image') }},
        {{ trx_json_extract('src', ['CLS_CODE'], type='varchar', alias='cls_code') }},
        {{ trx_json_extract('src', ['CLS_DESC'], type='varchar', alias='cls_desc') }},
        {{ trx_json_extract('src', ['CLS_ORG'], type='varchar', alias='cls_org') }},
        {{ trx_json_extract('src', ['CLS_RENTITYCODE'], type='varchar', alias='cls_rentitycode') }},
        {{ trx_json_extract('src', ['CLS_LEVEL'], type='float', alias='cls_level') }},
        {{ trx_json_extract('src', ['CLS_UPDATECOUNT'], type='float', alias='cls_updatecount') }},
        {{ trx_json_extract('src', ['CLS_CREATED'], type='timestamp_ntz', alias='cls_created') }},
        {{ trx_json_extract('src', ['CLS_UPDATED'], type='timestamp_ntz', alias='cls_updated') }},
        {{ trx_json_extract('src', ['CLS_NOTUSED'], type='varchar', alias='cls_notused') }},
        {{ trx_json_extract('src', ['CLS_ICON'], type='varchar', alias='cls_icon') }},
        {{ trx_json_extract('src', ['CLS_ICONPATH'], type='varchar', alias='cls_iconpath') }},
        {{ trx_json_extract('src', ['CLS_PROPERTY_DEFINITION'], type='varchar', alias='cls_property_definition') }},
        {{ trx_json_extract('src', ['CLS_MOBILE_NOTEBOOK_DEFINITION'], type='varchar', alias='cls_mobile_notebook_definition') }},
        {{ trx_json_extract('src', ['CLS_SEGMENTVALUE'], type='varchar', alias='cls_segmentvalue') }},
        {{ trx_json_extract('src', ['CLS_ENTITY'], type='varchar', alias='cls_entity') }},
        {{ trx_json_extract('src', ['CLS_RENTITY'], type='varchar', alias='cls_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5classes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cls_entity', 'cls_code', 'cls_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cls_lastsaved']) }}
