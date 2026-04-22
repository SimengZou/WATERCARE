{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ACDPARAMS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ADP_LASTSAVED'], type='timestamp_ntz', alias='adp_lastsaved') }},
        {{ trx_json_extract('src', ['ADP_DATATYPE'], type='varchar', alias='adp_datatype') }},
        {{ trx_json_extract('src', ['ADP_PROMPT'], type='varchar', alias='adp_prompt') }},
        {{ trx_json_extract('src', ['ADP_PROMPTX'], type='float', alias='adp_promptx') }},
        {{ trx_json_extract('src', ['ADP_PROMPTY'], type='float', alias='adp_prompty') }},
        {{ trx_json_extract('src', ['ADP_SEGMENTX'], type='float', alias='adp_segmentx') }},
        {{ trx_json_extract('src', ['ADP_SEGMENTY'], type='float', alias='adp_segmenty') }},
        {{ trx_json_extract('src', ['ADP_SEQ'], type='float', alias='adp_seq') }},
        {{ trx_json_extract('src', ['ADP_REQUIRED'], type='varchar', alias='adp_required') }},
        {{ trx_json_extract('src', ['ADP_DEFAULT'], type='varchar', alias='adp_default') }},
        {{ trx_json_extract('src', ['ADP_HINT'], type='varchar', alias='adp_hint') }},
        {{ trx_json_extract('src', ['ADP_LOVSQL'], type='varchar', alias='adp_lovsql') }},
        {{ trx_json_extract('src', ['ADP_DESCSQL'], type='varchar', alias='adp_descsql') }},
        {{ trx_json_extract('src', ['ADP_UPDATECOUNT'], type='float', alias='adp_updatecount') }},
        {{ trx_json_extract('src', ['ADP_QUERY'], type='varchar', alias='adp_query') }},
        {{ trx_json_extract('src', ['ADP_VALUELOOKUP'], type='varchar', alias='adp_valuelookup') }},
        {{ trx_json_extract('src', ['ADP_SEGMENT'], type='varchar', alias='adp_segment') }},
        {{ trx_json_extract('src', ['ADP_LENGTH'], type='float', alias='adp_length') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5acdparams') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['adp_segment']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['adp_lastsaved']) }}
