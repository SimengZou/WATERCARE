{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERDEFINEDFIELDDEPENDENCIES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UFD_LASTSAVED'], type='timestamp_ntz', alias='ufd_lastsaved') }},
        {{ trx_json_extract('src', ['UFD_PAGENAME'], type='varchar', alias='ufd_pagename') }},
        {{ trx_json_extract('src', ['UFD_LAYOUTCACHE'], type='varchar', alias='ufd_layoutcache') }},
        {{ trx_json_extract('src', ['UFD_UPDATECOUNT'], type='float', alias='ufd_updatecount') }},
        {{ trx_json_extract('src', ['UFD_RENTITY'], type='varchar', alias='ufd_rentity') }},
        {{ trx_json_extract('src', ['UFD_GRIDCACHE'], type='varchar', alias='ufd_gridcache') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userdefinedfielddependencies') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ufd_rentity', 'ufd_pagename']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ufd_lastsaved']) }}
