{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TRACKINGCTRL',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TKC_LASTSAVED'], type='timestamp_ntz', alias='tkc_lastsaved') }},
        {{ trx_json_extract('src', ['TKC_INTERFACERTYPE'], type='varchar', alias='tkc_interfacertype') }},
        {{ trx_json_extract('src', ['TKC_COLUMN'], type='varchar', alias='tkc_column') }},
        {{ trx_json_extract('src', ['TKC_RCOLUMN'], type='varchar', alias='tkc_rcolumn') }},
        {{ trx_json_extract('src', ['TKC_DISPLAYSEQ'], type='float', alias='tkc_displayseq') }},
        {{ trx_json_extract('src', ['TKC_LENGTH'], type='float', alias='tkc_length') }},
        {{ trx_json_extract('src', ['TKC_DATATYPE'], type='varchar', alias='tkc_datatype') }},
        {{ trx_json_extract('src', ['TKC_DATARTYPE'], type='varchar', alias='tkc_datartype') }},
        {{ trx_json_extract('src', ['TKC_INTERFACETYPE'], type='varchar', alias='tkc_interfacetype') }},
        {{ trx_json_extract('src', ['TKC_UPLOADCOLUMN'], type='varchar', alias='tkc_uploadcolumn') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5trackingctrl') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tkc_interfacertype', 'tkc_uploadcolumn']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tkc_lastsaved']) }}
