{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5LOCALE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LOC_LASTSAVED'], type='timestamp_ntz', alias='loc_lastsaved') }},
        {{ trx_json_extract('src', ['LOC_DECSYM'], type='varchar', alias='loc_decsym') }},
        {{ trx_json_extract('src', ['LOC_MONDECSYM'], type='varchar', alias='loc_mondecsym') }},
        {{ trx_json_extract('src', ['LOC_GRPSYM'], type='varchar', alias='loc_grpsym') }},
        {{ trx_json_extract('src', ['LOC_GRPDIGITS'], type='float', alias='loc_grpdigits') }},
        {{ trx_json_extract('src', ['LOC_FRACTDIGITS'], type='float', alias='loc_fractdigits') }},
        {{ trx_json_extract('src', ['LOC_MONFRACT'], type='float', alias='loc_monfract') }},
        {{ trx_json_extract('src', ['LOC_NEGSYM'], type='varchar', alias='loc_negsym') }},
        {{ trx_json_extract('src', ['LOC_POSSYM'], type='varchar', alias='loc_possym') }},
        {{ trx_json_extract('src', ['LOC_DATEFMT'], type='varchar', alias='loc_datefmt') }},
        {{ trx_json_extract('src', ['LOC_STARTDAY'], type='float', alias='loc_startday') }},
        {{ trx_json_extract('src', ['LOC_UPDATECOUNT'], type='float', alias='loc_updatecount') }},
        {{ trx_json_extract('src', ['LOC_MONTGRPSEPARATOR'], type='varchar', alias='loc_montgrpseparator') }},
        {{ trx_json_extract('src', ['LOC_MONTGRPDIGITS'], type='float', alias='loc_montgrpdigits') }},
        {{ trx_json_extract('src', ['LOC_CODE'], type='varchar', alias='loc_code') }},
        {{ trx_json_extract('src', ['LOC_DESC'], type='varchar', alias='loc_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5locale') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['loc_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['loc_lastsaved']) }}
