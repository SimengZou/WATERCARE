{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN Enum_localized Translations of enum descriptions, Generated 2026-04-10T19:40:58Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['keyword'], type='varchar', alias='keyword') }},
        {{ trx_json_extract('src', ['domain'], type='varchar', alias='domain') }},
        {{ trx_json_extract('src', ['const'], type='int', alias='const') }},
        {{ trx_json_extract('src', ['description', 'nl_NL'], type='varchar', alias='description__nl_nl') }},
        {{ trx_json_extract('src', ['description', 'en_US'], type='varchar', alias='description__en_us') }},
        {{ trx_json_extract('src', ['description', 'de_DE'], type='varchar', alias='description__de_de') }},
        {{ trx_json_extract('src', ['description', 'fr_FR'], type='varchar', alias='description__fr_fr') }},
        {{ trx_json_extract('src', ['description', 'es_ES'], type='varchar', alias='description__es_es') }},
        {{ trx_json_extract('src', ['description', 'it_IT'], type='varchar', alias='description__it_it') }},
        {{ trx_json_extract('src', ['description', 'sv_SE'], type='varchar', alias='description__sv_se') }},
        {{ trx_json_extract('src', ['description', 'hr_HR'], type='varchar', alias='description__hr_hr') }},
        {{ trx_json_extract('src', ['description', 'pt_PT'], type='varchar', alias='description__pt_pt') }},
        {{ trx_json_extract('src', ['description', 'ru_RU'], type='varchar', alias='description__ru_ru') }},
        {{ trx_json_extract('src', ['description', 'th_TH'], type='varchar', alias='description__th_th') }},
        {{ trx_json_extract('src', ['description', 'sr_BA'], type='varchar', alias='description__sr_ba') }},
        {{ trx_json_extract('src', ['description', 'ar_SA'], type='varchar', alias='description__ar_sa') }},
        {{ trx_json_extract('src', ['description', 'bg_BG'], type='varchar', alias='description__bg_bg') }},
        {{ trx_json_extract('src', ['description', 'pl_PL'], type='varchar', alias='description__pl_pl') }},
        {{ trx_json_extract('src', ['description', 'fi_FI'], type='varchar', alias='description__fi_fi') }},
        {{ trx_json_extract('src', ['description', 'he_IL'], type='varchar', alias='description__he_il') }},
        {{ trx_json_extract('src', ['description', 'ja_JP'], type='varchar', alias='description__ja_jp') }},
        {{ trx_json_extract('src', ['description', 'ko_KR'], type='varchar', alias='description__ko_kr') }},
        {{ trx_json_extract('src', ['description', 'hu_HU'], type='varchar', alias='description__hu_hu') }},
        {{ trx_json_extract('src', ['description', 'zh_TW'], type='varchar', alias='description__zh_tw') }},
        {{ trx_json_extract('src', ['description', 'zh_CN'], type='varchar', alias='description__zh_cn') }},
        {{ trx_json_extract('src', ['description', 'pt_BR'], type='varchar', alias='description__pt_br') }},
        {{ trx_json_extract('src', ['description', 'ro_RO'], type='varchar', alias='description__ro_ro') }},
        {{ trx_json_extract('src', ['description', 'sl_SI'], type='varchar', alias='description__sl_si') }},
        {{ trx_json_extract('src', ['description', 'cs_CZ'], type='varchar', alias='description__cs_cz') }},
        {{ trx_json_extract('src', ['description', 'uk_UA'], type='varchar', alias='description__uk_ua') }},
        {{ trx_json_extract('src', ['description', 'vi_VN'], type='varchar', alias='description__vi_vn') }},
        {{ trx_json_extract('src', ['description', 'sk_SK'], type='varchar', alias='description__sk_sk') }},
        {{ trx_json_extract('src', ['description', 'en_GB'], type='varchar', alias='description__en_gb') }},
        {{ trx_json_extract('src', ['description', 'tr_TR'], type='varchar', alias='description__tr_tr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_enum_localized') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['keyword']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}
