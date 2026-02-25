CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTTYPESETUP_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTTYPESETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPESETUPKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPESETUPKEY is not null) THEN
                    'ACCOUNTTYPESETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTYPESETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:ADDRESSNUMBERFORMAT is not null) THEN
                    'ADDRESSNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSNUMBERFORMAT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICATIONNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:APPLICATIONNUMBERFORMAT is not null) THEN
                    'APPLICATIONNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:APPLICATIONNUMBERFORMAT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CATEGORY), '0'), 38, 10) is null and 
                    src:CATEGORY is not null) THEN
                    'CATEGORY contains a non-numeric value : \'' || AS_VARCHAR(src:CATEGORY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DETAILPAGE), '0'), 38, 10) is null and 
                    src:DETAILPAGE is not null) THEN
                    'DETAILPAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DETAILPAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:IDENTITYNUMBERFORMAT is not null) THEN
                    'IDENTITYNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:IDENTITYNUMBERFORMAT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:PARCELNUMBERFORMAT is not null) THEN
                    'PARCELNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:PARCELNUMBERFORMAT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:PROPERTYNUMBERFORMAT is not null) THEN
                    'PROPERTYNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYNUMBERFORMAT) || '\'' WHEN 
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
                                    
                src:ACCOUNTTYPESETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTTYPESETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPESETUPKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPESETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:ADDRESSNUMBERFORMAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICATIONNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:APPLICATIONNUMBERFORMAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CATEGORY), '0'), 38, 10) is null and 
                    src:CATEGORY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DETAILPAGE), '0'), 38, 10) is null and 
                    src:DETAILPAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:IDENTITYNUMBERFORMAT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:PARCELNUMBERFORMAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:PROPERTYNUMBERFORMAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)