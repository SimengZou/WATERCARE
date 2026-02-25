create or replace view VW_ICARE_ALL_INJURIES_API_JSON as 

select

value:elbow::varchar as "ELBOW",

value:lower_leg::varchar as "LOWER_LEG",

value:head_skull::varchar as "HEAD_SKULL",

value:thumb::varchar as "THUMB",

value:abdomen::varchar as "ABDOMEN",

value:injury_cause::varchar as "INJURY_CAUSE",

value:skin::varchar as "SKIN",

value:respiratory_system::varchar as "RESPIRATORY_SYSTEM",

TRY_TO_TIMESTAMP_NTZ(value:sys_updated_on::varchar) as "SYS_UPDATED_ON",

value:fingers::varchar as "FINGERS",

value:wrist::varchar as "WRIST",

value:hip::varchar as "HIP",

value:injury_type::varchar as "INJURY_TYPE",

value:sys_id::varchar as "SYS_ID",

value:sys_updated_by::varchar as "SYS_UPDATED_BY",

TRY_TO_TIMESTAMP_NTZ(value:sys_created_on::varchar) as "SYS_CREATED_ON",

value:ear::varchar as "EAR",

value:digestive_system::varchar as "DIGESTIVE_SYSTEM",

value:shoulder::varchar as "SHOULDER",

value:whole_body::varchar as "WHOLE_BODY",

value:ankle::varchar as "ANKLE",

value:foot::varchar as "FOOT",

value:sys_created_by::varchar as "SYS_CREATED_BY",

value:hand::varchar as "HAND",

value:toes::varchar as "TOES",

value:internal_organs::varchar as "INTERNAL_ORGANS",

value:nose::varchar as "NOSE",

value:upper_leg::varchar as "UPPER_LEG",

value:chest::varchar as "CHEST",

TRY_TO_NUMBER(value:sys_mod_count::varchar) as "SYS_MOD_COUNT",

value:groin::varchar as "GROIN",

value:brain::varchar as "BRAIN",

value:neck::varchar as "NECK",

value:sys_tags::varchar as "SYS_TAGS",

value:pyschological::varchar as "PYSCHOLOGICAL",

value:upper_arm::varchar as "UPPER_ARM",

value:upper_back::varchar as "UPPER_BACK",

value:eye::varchar as "EYE",

value:knee::varchar as "KNEE",

value:face::varchar as "FACE",

value:mouth::varchar as "MOUTH",

value:external_user::varchar as "EXTERNAL_USER",

value:forearm::varchar as "FOREARM",

value:incident::varchar as "INCIDENT",

value:ribs::varchar as "RIBS",

value:user::varchar as "USER",

value:circulatory_system::varchar as "CIRCULATORY_SYSTEM",

value:lower_back::varchar as "LOWER_BACK"
--,SYSDATE() as ETL_LOAD_DATETIME


  from

    DATAHUB_LANDING.ICARE_ALL_INJURIES_API 

  , lateral flatten( input => src:result );