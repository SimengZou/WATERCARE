CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYGROUP_ERROR AS SELECT src, 'IPS_BILLING_DELINQUENCYGROUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYSETUPKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYSETUPKEY is not null) THEN
                    'DELINQUENCYSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) THEN
                    'DISPLAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTRYSCHEMEFORMULA), '0'), 38, 10) is null and 
                    src:ENTRYSCHEMEFORMULA is not null) THEN
                    'ENTRYSCHEMEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:ENTRYSCHEMEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) THEN
                    'GROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GROUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINENTRYAMT), '0'), 38, 10) is null and 
                    src:MINENTRYAMT is not null) THEN
                    'MINENTRYAMT contains a non-numeric value : \'' || AS_VARCHAR(src:MINENTRYAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:GROUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DELINQUENCYGROUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYSETUPKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTRYSCHEMEFORMULA), '0'), 38, 10) is null and 
                    src:ENTRYSCHEMEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINENTRYAMT), '0'), 38, 10) is null and 
                    src:MINENTRYAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)