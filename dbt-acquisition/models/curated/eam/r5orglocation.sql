{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ORGLOCATION',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OGL_LASTSAVED'], type='timestamp_ntz', alias='ogl_lastsaved') }},
        {{ trx_json_extract('src', ['OGL_BODGROUP'], type='varchar', alias='ogl_bodgroup') }},
        {{ trx_json_extract('src', ['OGL_UPDATECOUNT'], type='float', alias='ogl_updatecount') }},
        {{ trx_json_extract('src', ['OGL_ORG'], type='varchar', alias='ogl_org') }},
        {{ trx_json_extract('src', ['OGL_ENTERPRISELOCATION'], type='varchar', alias='ogl_enterpriselocation') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5orglocation') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ogl_org', 'ogl_bodgroup']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ogl_lastsaved']) }}
