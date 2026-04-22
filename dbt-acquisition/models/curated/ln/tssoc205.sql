{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tssoc205 Service Engineer Assignments table, Generated 2026-04-10T19:42:38Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['orgn'], type='int', alias='orgn') }},
        {{ trx_json_extract('src', ['orgn_kw'], type='varchar', alias='orgn_kw') }},
        {{ trx_json_extract('src', ['orno'], type='varchar', alias='orno') }},
        {{ trx_json_extract('src', ['line'], type='int', alias='line') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['acln'], type='int', alias='acln') }},
        {{ trx_json_extract('src', ['stat'], type='int', alias='stat') }},
        {{ trx_json_extract('src', ['stat_kw'], type='varchar', alias='stat_kw') }},
        {{ trx_json_extract('src', ['dltm'], type='timestamp_ntz', alias='dltm') }},
        {{ trx_json_extract('src', ['actm'], type='timestamp_ntz', alias='actm') }},
        {{ trx_json_extract('src', ['rjtm'], type='timestamp_ntz', alias='rjtm') }},
        {{ trx_json_extract('src', ['rejr'], type='varchar', alias='rejr') }},
        {{ trx_json_extract('src', ['pstm'], type='timestamp_ntz', alias='pstm') }},
        {{ trx_json_extract('src', ['pftm'], type='timestamp_ntz', alias='pftm') }},
        {{ trx_json_extract('src', ['ptst'], type='timestamp_ntz', alias='ptst') }},
        {{ trx_json_extract('src', ['ptft'], type='timestamp_ntz', alias='ptft') }},
        {{ trx_json_extract('src', ['astm'], type='timestamp_ntz', alias='astm') }},
        {{ trx_json_extract('src', ['aftm'], type='timestamp_ntz', alias='aftm') }},
        {{ trx_json_extract('src', ['atst'], type='timestamp_ntz', alias='atst') }},
        {{ trx_json_extract('src', ['atft'], type='timestamp_ntz', alias='atft') }},
        {{ trx_json_extract('src', ['jctm'], type='timestamp_ntz', alias='jctm') }},
        {{ trx_json_extract('src', ['ltct'], type='timestamp_ntz', alias='ltct') }},
        {{ trx_json_extract('src', ['esdt'], type='timestamp_ntz', alias='esdt') }},
        {{ trx_json_extract('src', ['acdu'], type='float', alias='acdu') }},
        {{ trx_json_extract('src', ['trdu'], type='float', alias='trdu') }},
        {{ trx_json_extract('src', ['trdi'], type='float', alias='trdi') }},
        {{ trx_json_extract('src', ['uecp'], type='int', alias='uecp') }},
        {{ trx_json_extract('src', ['uecp_kw'], type='varchar', alias='uecp_kw') }},
        {{ trx_json_extract('src', ['pged'], type='int', alias='pged') }},
        {{ trx_json_extract('src', ['pged_kw'], type='varchar', alias='pged_kw') }},
        {{ trx_json_extract('src', ['pgrd'], type='int', alias='pgrd') }},
        {{ trx_json_extract('src', ['pgrd_kw'], type='varchar', alias='pgrd_kw') }},
        {{ trx_json_extract('src', ['actp'], type='int', alias='actp') }},
        {{ trx_json_extract('src', ['actp_kw'], type='varchar', alias='actp_kw') }},
        {{ trx_json_extract('src', ['pgit'], type='int', alias='pgit') }},
        {{ trx_json_extract('src', ['pgdt'], type='timestamp_ntz', alias='pgdt') }},
        {{ trx_json_extract('src', ['pgcn'], type='int', alias='pgcn') }},
        {{ trx_json_extract('src', ['txta'], type='int', alias='txta') }},
        {{ trx_json_extract('src', ['cmbb_orno_acln_ref_compnr'], type='int', alias='cmbb_orno_acln_ref_compnr') }},
        {{ trx_json_extract('src', ['cmbc_orno_acln_ref_compnr'], type='int', alias='cmbc_orno_acln_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['rejr_ref_compnr'], type='int', alias='rejr_ref_compnr') }},
        {{ trx_json_extract('src', ['txta_ref_compnr'], type='int', alias='txta_ref_compnr') }},
        {{ trx_json_extract('src', ['trdi_buln'], type='float', alias='trdi_buln') }},
        {{ trx_json_extract('src', ['trdu_butm'], type='float', alias='trdu_butm') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tssoc205') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'orgn', 'orno', 'line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
