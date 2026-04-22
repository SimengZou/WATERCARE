{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5ESCDETAILS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['UPDATECOUNT'], type='float', alias='updatecount') }},
        {{ trx_json_extract('src', ['ESC_ASSETLOCATION'], type='varchar', alias='esc_assetlocation') }},
        {{ trx_json_extract('src', ['ESC_TELEPHONE'], type='varchar', alias='esc_telephone') }},
        {{ trx_json_extract('src', ['ESC_PERSONS'], type='varchar', alias='esc_persons') }},
        {{ trx_json_extract('src', ['ESC_ADDITIONS'], type='varchar', alias='esc_additions') }},
        {{ trx_json_extract('src', ['ESC_ALERATION'], type='varchar', alias='esc_aleration') }},
        {{ trx_json_extract('src', ['ESC_NEWWORK'], type='varchar', alias='esc_newwork') }},
        {{ trx_json_extract('src', ['ESC_MAINTENANCE'], type='varchar', alias='esc_maintenance') }},
        {{ trx_json_extract('src', ['ESC_REPAIRS'], type='varchar', alias='esc_repairs') }},
        {{ trx_json_extract('src', ['ESC_ANYONE'], type='varchar', alias='esc_anyone') }},
        {{ trx_json_extract('src', ['ESC_WHO'], type='varchar', alias='esc_who') }},
        {{ trx_json_extract('src', ['ESC_MAINS'], type='varchar', alias='esc_mains') }},
        {{ trx_json_extract('src', ['ESC_SWITCH'], type='varchar', alias='esc_switch') }},
        {{ trx_json_extract('src', ['ESC_EARTH'], type='varchar', alias='esc_earth') }},
        {{ trx_json_extract('src', ['ESC_LINES'], type='varchar', alias='esc_lines') }},
        {{ trx_json_extract('src', ['ESC_DESC'], type='varchar', alias='esc_desc') }},
        {{ trx_json_extract('src', ['ESC_IAMITEM'], type='varchar', alias='esc_iamitem') }},
        {{ trx_json_extract('src', ['ESC_PUMP'], type='varchar', alias='esc_pump') }},
        {{ trx_json_extract('src', ['ESC_VALVE'], type='varchar', alias='esc_valve') }},
        {{ trx_json_extract('src', ['ESC_INSTRUMENTS'], type='varchar', alias='esc_instruments') }},
        {{ trx_json_extract('src', ['ESC_GENERATOR'], type='varchar', alias='esc_generator') }},
        {{ trx_json_extract('src', ['ESC_SBOARD'], type='varchar', alias='esc_sboard') }},
        {{ trx_json_extract('src', ['ESC_PFACTOR'], type='varchar', alias='esc_pfactor') }},
        {{ trx_json_extract('src', ['ESC_MSTARTER'], type='varchar', alias='esc_mstarter') }},
        {{ trx_json_extract('src', ['ESC_LOUTLET'], type='varchar', alias='esc_loutlet') }},
        {{ trx_json_extract('src', ['ESC_RANGE'], type='varchar', alias='esc_range') }},
        {{ trx_json_extract('src', ['ESC_MOTOR'], type='varchar', alias='esc_motor') }},
        {{ trx_json_extract('src', ['ESC_FANS'], type='varchar', alias='esc_fans') }},
        {{ trx_json_extract('src', ['ESC_PLC'], type='varchar', alias='esc_plc') }},
        {{ trx_json_extract('src', ['ESC_ISOLATORS'], type='varchar', alias='esc_isolators') }},
        {{ trx_json_extract('src', ['ESC_PANELS'], type='varchar', alias='esc_panels') }},
        {{ trx_json_extract('src', ['ESC_UPS'], type='varchar', alias='esc_ups') }},
        {{ trx_json_extract('src', ['ESC_VSD'], type='varchar', alias='esc_vsd') }},
        {{ trx_json_extract('src', ['ESC_SOUTLETS'], type='varchar', alias='esc_soutlets') }},
        {{ trx_json_extract('src', ['ESC_WHEATERS'], type='varchar', alias='esc_wheaters') }},
        {{ trx_json_extract('src', ['ESC_OTHERS'], type='varchar', alias='esc_others') }},
        {{ trx_json_extract('src', ['ESC_COMP1'], type='varchar', alias='esc_comp1') }},
        {{ trx_json_extract('src', ['ESC_COMP2'], type='varchar', alias='esc_comp2') }},
        {{ trx_json_extract('src', ['ESC_REG58'], type='varchar', alias='esc_reg58') }},
        {{ trx_json_extract('src', ['ESC_EARTHRATE'], type='varchar', alias='esc_earthrate') }},
        {{ trx_json_extract('src', ['ESC_FITTINGS'], type='varchar', alias='esc_fittings') }},
        {{ trx_json_extract('src', ['ESC_SUPPLIERS'], type='varchar', alias='esc_suppliers') }},
        {{ trx_json_extract('src', ['ESC_MANUFACTURER'], type='varchar', alias='esc_manufacturer') }},
        {{ trx_json_extract('src', ['ESC_REGLATNS'], type='varchar', alias='esc_reglatns') }},
        {{ trx_json_extract('src', ['ESC_SAFE'], type='varchar', alias='esc_safe') }},
        {{ trx_json_extract('src', ['ESC_OTHERCOMP'], type='varchar', alias='esc_othercomp') }},
        {{ trx_json_extract('src', ['ESC_POLARITY'], type='varchar', alias='esc_polarity') }},
        {{ trx_json_extract('src', ['ESC_INSULATION'], type='varchar', alias='esc_insulation') }},
        {{ trx_json_extract('src', ['ESC_CONTINUITY'], type='varchar', alias='esc_continuity') }},
        {{ trx_json_extract('src', ['ESC_BONDING'], type='varchar', alias='esc_bonding') }},
        {{ trx_json_extract('src', ['ESC_IMPEDENCE'], type='varchar', alias='esc_impedence') }},
        {{ trx_json_extract('src', ['ESC_VISUAL'], type='varchar', alias='esc_visual') }},
        {{ trx_json_extract('src', ['ESC_OTHERTEST'], type='varchar', alias='esc_othertest') }},
        {{ trx_json_extract('src', ['ESC_ELEREF'], type='varchar', alias='esc_eleref') }},
        {{ trx_json_extract('src', ['CREATEDBY'], type='varchar', alias='createdby') }},
        {{ trx_json_extract('src', ['CREATED'], type='timestamp_ntz', alias='created') }},
        {{ trx_json_extract('src', ['UPDATEDBY'], type='varchar', alias='updatedby') }},
        {{ trx_json_extract('src', ['UPDATED'], type='timestamp_ntz', alias='updated') }},
        {{ trx_json_extract('src', ['ESC_CODE'], type='varchar', alias='esc_code') }},
        {{ trx_json_extract('src', ['ESC_PEW'], type='varchar', alias='esc_pew') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5escdetails') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['esc_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}
