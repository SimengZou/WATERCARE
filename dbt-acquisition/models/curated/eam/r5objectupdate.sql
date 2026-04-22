{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5OBJECTUPDATE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OUP_LASTSAVED'], type='timestamp_ntz', alias='oup_lastsaved') }},
        {{ trx_json_extract('src', ['OUP_OLDCODE'], type='varchar', alias='oup_oldcode') }},
        {{ trx_json_extract('src', ['OUP_ORG'], type='varchar', alias='oup_org') }},
        {{ trx_json_extract('src', ['OUP_USER'], type='varchar', alias='oup_user') }},
        {{ trx_json_extract('src', ['OUP_DATE'], type='timestamp_ntz', alias='oup_date') }},
        {{ trx_json_extract('src', ['OUP_PK'], type='float', alias='oup_pk') }},
        {{ trx_json_extract('src', ['OUP_NEWCODE'], type='varchar', alias='oup_newcode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5objectupdate') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['oup_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['oup_lastsaved']) }}
