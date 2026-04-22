{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PREFILTERLOOKUP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PFE_LASTSAVED'], type='timestamp_ntz', alias='pfe_lastsaved') }},
        {{ trx_json_extract('src', ['PFE_ELEMENTID'], type='varchar', alias='pfe_elementid') }},
        {{ trx_json_extract('src', ['PFE_USERFILTER'], type='varchar', alias='pfe_userfilter') }},
        {{ trx_json_extract('src', ['PFE_UPDATECOUNT'], type='float', alias='pfe_updatecount') }},
        {{ trx_json_extract('src', ['PFE_PAGENAME'], type='varchar', alias='pfe_pagename') }},
        {{ trx_json_extract('src', ['PFE_FILTERSTRXML'], type='varchar', alias='pfe_filterstrxml') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5prefilterlookup') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pfe_pagename', 'pfe_elementid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pfe_lastsaved']) }}
