{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ROUTES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ROU_LASTSAVED'], type='timestamp_ntz', alias='rou_lastsaved') }},
        {{ trx_json_extract('src', ['ROU_DESC'], type='varchar', alias='rou_desc') }},
        {{ trx_json_extract('src', ['ROU_CATEGORY'], type='varchar', alias='rou_category') }},
        {{ trx_json_extract('src', ['ROU_TEMPLATE'], type='varchar', alias='rou_template') }},
        {{ trx_json_extract('src', ['ROU_CLASS'], type='varchar', alias='rou_class') }},
        {{ trx_json_extract('src', ['ROU_REVISION'], type='float', alias='rou_revision') }},
        {{ trx_json_extract('src', ['ROU_REVSTATUS'], type='varchar', alias='rou_revstatus') }},
        {{ trx_json_extract('src', ['ROU_ORG'], type='varchar', alias='rou_org') }},
        {{ trx_json_extract('src', ['ROU_CLASS_ORG'], type='varchar', alias='rou_class_org') }},
        {{ trx_json_extract('src', ['ROU_UPDATECOUNT'], type='float', alias='rou_updatecount') }},
        {{ trx_json_extract('src', ['ROU_LINKGISWO'], type='varchar', alias='rou_linkgiswo') }},
        {{ trx_json_extract('src', ['ROU_CODE'], type='varchar', alias='rou_code') }},
        {{ trx_json_extract('src', ['ROU_REVRSTATUS'], type='varchar', alias='rou_revrstatus') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5routes') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rou_code', 'rou_revision']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rou_lastsaved']) }}
