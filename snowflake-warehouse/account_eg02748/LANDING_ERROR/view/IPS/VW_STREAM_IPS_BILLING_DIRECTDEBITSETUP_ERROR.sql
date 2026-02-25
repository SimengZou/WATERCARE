CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DIRECTDEBITSETUP_ERROR AS SELECT src, 'IPS_BILLING_DIRECTDEBITSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE1DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE1DAY is not null) THEN
                    'DIRECTDEBITCYCLE1DAY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITCYCLE1DAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE2DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE2DAY is not null) THEN
                    'DIRECTDEBITCYCLE2DAY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITCYCLE2DAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE3DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE3DAY is not null) THEN
                    'DIRECTDEBITCYCLE3DAY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITCYCLE3DAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE4DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE4DAY is not null) THEN
                    'DIRECTDEBITCYCLE4DAY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITCYCLE4DAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLENUMBER), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLENUMBER is not null) THEN
                    'DIRECTDEBITCYCLENUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITCYCLENUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITSETUPKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITSETUPKEY is not null) THEN
                    'DIRECTDEBITSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMAMOUNT), '0'), 38, 10) is null and 
                    src:MAXIMUMAMOUNT is not null) THEN
                    'MAXIMUMAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMSUSPENSIONDAY), '0'), 38, 10) is null and 
                    src:MAXIMUMSUSPENSIONDAY is not null) THEN
                    'MAXIMUMSUSPENSIONDAY contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMSUSPENSIONDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMSUSPENSIONNUMBER), '0'), 38, 10) is null and 
                    src:MAXIMUMSUSPENSIONNUMBER is not null) THEN
                    'MAXIMUMSUSPENSIONNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMSUSPENSIONNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMAMOUNT), '0'), 38, 10) is null and 
                    src:MINIMUMAMOUNT is not null) THEN
                    'MINIMUMAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MINIMUMAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTEEXTRACTDATEDAYS), '0'), 38, 10) is null and 
                    src:PRENOTEEXTRACTDATEDAYS is not null) THEN
                    'PRENOTEEXTRACTDATEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:PRENOTEEXTRACTDATEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTENUMBEROFDAYS), '0'), 38, 10) is null and 
                    src:PRENOTENUMBEROFDAYS is not null) THEN
                    'PRENOTENUMBEROFDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:PRENOTENUMBEROFDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:DIRECTDEBITSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DIRECTDEBITSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE1DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE1DAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE2DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE2DAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE3DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE3DAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLE4DAY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLE4DAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITCYCLENUMBER), '0'), 38, 10) is null and 
                    src:DIRECTDEBITCYCLENUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITSETUPKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMAMOUNT), '0'), 38, 10) is null and 
                    src:MAXIMUMAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMSUSPENSIONDAY), '0'), 38, 10) is null and 
                    src:MAXIMUMSUSPENSIONDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMSUSPENSIONNUMBER), '0'), 38, 10) is null and 
                    src:MAXIMUMSUSPENSIONNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMAMOUNT), '0'), 38, 10) is null and 
                    src:MINIMUMAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTEEXTRACTDATEDAYS), '0'), 38, 10) is null and 
                    src:PRENOTEEXTRACTDATEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTENUMBEROFDAYS), '0'), 38, 10) is null and 
                    src:PRENOTENUMBEROFDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)