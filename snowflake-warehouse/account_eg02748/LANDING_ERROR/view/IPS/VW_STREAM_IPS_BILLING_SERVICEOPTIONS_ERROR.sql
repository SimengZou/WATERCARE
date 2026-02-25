CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_SERVICEOPTIONS_ERROR AS SELECT src, 'IPS_BILLING_SERVICEOPTIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) THEN
                    'ASSETTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) THEN
                    'CONSUMPTIONPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CONSUMPTIONPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LEARNMOREKBKEY), '0'), 38, 10) is null and 
                    src:LEARNMOREKBKEY is not null) THEN
                    'LEARNMOREKBKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LEARNMOREKBKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:METERTYPEFORMULA), '0'), 38, 10) is null and 
                    src:METERTYPEFORMULA is not null) THEN
                    'METERTYPEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:METERTYPEFORMULA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINACTKEY), '0'), 38, 10) is null and 
                    src:MOVEINACTKEY is not null) THEN
                    'MOVEINACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINCOLOR), '0'), 38, 10) is null and 
                    src:MOVEINCOLOR is not null) THEN
                    'MOVEINCOLOR contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINCOLOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINREQSKEY), '0'), 38, 10) is null and 
                    src:MOVEINREQSKEY is not null) THEN
                    'MOVEINREQSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINREQSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSRWOCONDITIONFORMULA), '0'), 38, 10) is null and 
                    src:MOVEINSRWOCONDITIONFORMULA is not null) THEN
                    'MOVEINSRWOCONDITIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINSRWOCONDITIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSWOKEY), '0'), 38, 10) is null and 
                    src:MOVEINSWOKEY is not null) THEN
                    'MOVEINSWOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINSWOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTACTKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTACTKEY is not null) THEN
                    'MOVEOUTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTLEARNMOREKBKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTLEARNMOREKBKEY is not null) THEN
                    'MOVEOUTLEARNMOREKBKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTLEARNMOREKBKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTREQSKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTREQSKEY is not null) THEN
                    'MOVEOUTREQSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTREQSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSRWOCONDITIONFORMULA), '0'), 38, 10) is null and 
                    src:MOVEOUTSRWOCONDITIONFORMULA is not null) THEN
                    'MOVEOUTSRWOCONDITIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTSRWOCONDITIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSWOKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTSWOKEY is not null) THEN
                    'MOVEOUTSWOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTSWOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEDEFINITIONKEY), '0'), 38, 10) is null and 
                    src:SERVICEDEFINITIONKEY is not null) THEN
                    'SERVICEDEFINITIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEDEFINITIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD1KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD1KEY is not null) THEN
                    'SERVICEFIELD1KEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD1KEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD2KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD2KEY is not null) THEN
                    'SERVICEFIELD2KEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD2KEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD3KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD3KEY is not null) THEN
                    'SERVICEFIELD3KEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEFIELD3KEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) THEN
                    'SERVICEOPTIONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEOPTIONSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRADEWASTEMAX), '0'), 38, 10) is null and 
                    src:TRADEWASTEMAX is not null) THEN
                    'TRADEWASTEMAX contains a non-numeric value : \'' || AS_VARCHAR(src:TRADEWASTEMAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRADEWASTERATE), '0'), 38, 10) is null and 
                    src:TRADEWASTERATE is not null) THEN
                    'TRADEWASTERATE contains a non-numeric value : \'' || AS_VARCHAR(src:TRADEWASTERATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WINTERAVGSETUPKEY), '0'), 38, 10) is null and 
                    src:WINTERAVGSETUPKEY is not null) THEN
                    'WINTERAVGSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WINTERAVGSETUPKEY) || '\'' WHEN 
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
                                    
                src:SERVICEOPTIONSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_SERVICEOPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LEARNMOREKBKEY), '0'), 38, 10) is null and 
                    src:LEARNMOREKBKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:METERTYPEFORMULA), '0'), 38, 10) is null and 
                    src:METERTYPEFORMULA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINACTKEY), '0'), 38, 10) is null and 
                    src:MOVEINACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINCOLOR), '0'), 38, 10) is null and 
                    src:MOVEINCOLOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINREQSKEY), '0'), 38, 10) is null and 
                    src:MOVEINREQSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSRWOCONDITIONFORMULA), '0'), 38, 10) is null and 
                    src:MOVEINSRWOCONDITIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSWOKEY), '0'), 38, 10) is null and 
                    src:MOVEINSWOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTACTKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTLEARNMOREKBKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTLEARNMOREKBKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTREQSKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTREQSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSRWOCONDITIONFORMULA), '0'), 38, 10) is null and 
                    src:MOVEOUTSRWOCONDITIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSWOKEY), '0'), 38, 10) is null and 
                    src:MOVEOUTSWOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEDEFINITIONKEY), '0'), 38, 10) is null and 
                    src:SERVICEDEFINITIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD1KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD1KEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD2KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD2KEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEFIELD3KEY), '0'), 38, 10) is null and 
                    src:SERVICEFIELD3KEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRADEWASTEMAX), '0'), 38, 10) is null and 
                    src:TRADEWASTEMAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRADEWASTERATE), '0'), 38, 10) is null and 
                    src:TRADEWASTERATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WINTERAVGSETUPKEY), '0'), 38, 10) is null and 
                    src:WINTERAVGSETUPKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)