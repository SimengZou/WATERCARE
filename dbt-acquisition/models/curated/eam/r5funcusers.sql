{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FUNCUSERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FUS_LASTSAVED'], type='timestamp_ntz', alias='fus_lastsaved') }},
        {{ trx_json_extract('src', ['FUS_SHORTN'], type='varchar', alias='fus_shortn') }},
        {{ trx_json_extract('src', ['FUS_PRINTER'], type='varchar', alias='fus_printer') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN1'], type='varchar', alias='fus_butfun1') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN2'], type='varchar', alias='fus_butfun2') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN3'], type='varchar', alias='fus_butfun3') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN4'], type='varchar', alias='fus_butfun4') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN5'], type='varchar', alias='fus_butfun5') }},
        {{ trx_json_extract('src', ['FUS_BUTFUN6'], type='varchar', alias='fus_butfun6') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME1'], type='varchar', alias='fus_butname1') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME2'], type='varchar', alias='fus_butname2') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME3'], type='varchar', alias='fus_butname3') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME4'], type='varchar', alias='fus_butname4') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME5'], type='varchar', alias='fus_butname5') }},
        {{ trx_json_extract('src', ['FUS_BUTNAME6'], type='varchar', alias='fus_butname6') }},
        {{ trx_json_extract('src', ['FUS_BUTICON1'], type='varchar', alias='fus_buticon1') }},
        {{ trx_json_extract('src', ['FUS_BUTICON2'], type='varchar', alias='fus_buticon2') }},
        {{ trx_json_extract('src', ['FUS_BUTICON3'], type='varchar', alias='fus_buticon3') }},
        {{ trx_json_extract('src', ['FUS_BUTICON4'], type='varchar', alias='fus_buticon4') }},
        {{ trx_json_extract('src', ['FUS_BUTICON5'], type='varchar', alias='fus_buticon5') }},
        {{ trx_json_extract('src', ['FUS_BUTICON6'], type='varchar', alias='fus_buticon6') }},
        {{ trx_json_extract('src', ['FUS_NOTES'], type='varchar', alias='fus_notes') }},
        {{ trx_json_extract('src', ['FUS_FILTER'], type='varchar', alias='fus_filter') }},
        {{ trx_json_extract('src', ['FUS_UPDATECOUNT'], type='float', alias='fus_updatecount') }},
        {{ trx_json_extract('src', ['FUS_FUNCTION'], type='varchar', alias='fus_function') }},
        {{ trx_json_extract('src', ['FUS_USER'], type='varchar', alias='fus_user') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5funcusers') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fus_user', 'fus_function']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fus_lastsaved']) }}
