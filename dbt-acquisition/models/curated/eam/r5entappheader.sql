{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ENTAPPHEADER',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EAH_LASTSAVED'], type='timestamp_ntz', alias='eah_lastsaved') }},
        {{ trx_json_extract('src', ['EAH_RENTITY'], type='varchar', alias='eah_rentity') }},
        {{ trx_json_extract('src', ['EAH_ENTITY'], type='varchar', alias='eah_entity') }},
        {{ trx_json_extract('src', ['EAH_CODE'], type='varchar', alias='eah_code') }},
        {{ trx_json_extract('src', ['EAH_REVISION'], type='float', alias='eah_revision') }},
        {{ trx_json_extract('src', ['EAH_APPDATE'], type='timestamp_ntz', alias='eah_appdate') }},
        {{ trx_json_extract('src', ['EAH_USER'], type='varchar', alias='eah_user') }},
        {{ trx_json_extract('src', ['EAH_DATE'], type='timestamp_ntz', alias='eah_date') }},
        {{ trx_json_extract('src', ['EAH_REASON'], type='varchar', alias='eah_reason') }},
        {{ trx_json_extract('src', ['EAH_UPDATECOUNT'], type='float', alias='eah_updatecount') }},
        {{ trx_json_extract('src', ['EAH_PK'], type='float', alias='eah_pk') }},
        {{ trx_json_extract('src', ['EAH_APPLIST'], type='varchar', alias='eah_applist') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5entappheader') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['eah_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['eah_lastsaved']) }}
