{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tfgld170 Taxonomies table, Generated 2026-04-10T19:41:42Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['taxo'], type='varchar', alias='taxo') }},
        {{ trx_json_extract('src', ['vers'], type='int', alias='vers') }},
        {{ trx_json_extract('src', ['desc', 'en_US'], type='varchar', alias='desc__en_us') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['duac'], type='int', alias='duac') }},
        {{ trx_json_extract('src', ['duac_kw'], type='varchar', alias='duac_kw') }},
        {{ trx_json_extract('src', ['acom'], type='int', alias='acom') }},
        {{ trx_json_extract('src', ['acom_kw'], type='varchar', alias='acom_kw') }},
        {{ trx_json_extract('src', ['dimm'], type='int', alias='dimm') }},
        {{ trx_json_extract('src', ['dimm_kw'], type='varchar', alias='dimm_kw') }},
        {{ trx_json_extract('src', ['usrc'], type='varchar', alias='usrc') }},
        {{ trx_json_extract('src', ['crdt'], type='timestamp_ntz', alias='crdt') }},
        {{ trx_json_extract('src', ['usrm'], type='varchar', alias='usrm') }},
        {{ trx_json_extract('src', ['datm'], type='timestamp_ntz', alias='datm') }},
        {{ trx_json_extract('src', ['usrr'], type='varchar', alias='usrr') }},
        {{ trx_json_extract('src', ['rldt'], type='timestamp_ntz', alias='rldt') }},
        {{ trx_json_extract('src', ['usre'], type='varchar', alias='usre') }},
        {{ trx_json_extract('src', ['cldt'], type='timestamp_ntz', alias='cldt') }},
        {{ trx_json_extract('src', ['eacc'], type='int', alias='eacc') }},
        {{ trx_json_extract('src', ['eacc_kw'], type='varchar', alias='eacc_kw') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tfgld170') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'taxo', 'vers']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
