{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ERRSOURCE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ERS_LASTSAVED'], type='timestamp_ntz', alias='ers_lastsaved') }},
        {{ trx_json_extract('src', ['ERS_TYPE'], type='varchar', alias='ers_type') }},
        {{ trx_json_extract('src', ['ERS_DESC'], type='varchar', alias='ers_desc') }},
        {{ trx_json_extract('src', ['ERS_CODE'], type='varchar', alias='ers_code') }},
        {{ trx_json_extract('src', ['ERS_NAME'], type='varchar', alias='ers_name') }},
        {{ trx_json_extract('src', ['ERS_UPDATECOUNT'], type='float', alias='ers_updatecount') }},
        {{ trx_json_extract('src', ['ERS_SOURCE'], type='varchar', alias='ers_source') }},
        {{ trx_json_extract('src', ['ERS_NUMBER'], type='float', alias='ers_number') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5errsource') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ers_source', 'ers_type', 'ers_number']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ers_lastsaved']) }}
