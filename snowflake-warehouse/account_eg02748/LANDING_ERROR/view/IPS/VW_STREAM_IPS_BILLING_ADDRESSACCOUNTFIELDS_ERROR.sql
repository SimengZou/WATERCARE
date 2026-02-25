CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ADDRESSACCOUNTFIELDS_ERROR AS SELECT src, 'IPS_BILLING_ADDRESSACCOUNTFIELDS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSACCTFIELDSKEY), '0'), 38, 10) is null and 
                    src:ADDRESSACCTFIELDSKEY is not null) THEN
                    'ADDRESSACCTFIELDSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSACCTFIELDSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXACTIVEOWNERS), '0'), 38, 10) is null and 
                    src:MAXACTIVEOWNERS is not null) THEN
                    'MAXACTIVEOWNERS contains a non-numeric value : \'' || AS_VARCHAR(src:MAXACTIVEOWNERS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXACTIVETENANTS), '0'), 38, 10) is null and 
                    src:MAXACTIVETENANTS is not null) THEN
                    'MAXACTIVETENANTS contains a non-numeric value : \'' || AS_VARCHAR(src:MAXACTIVETENANTS) || '\'' WHEN 
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
                                    
                src:ADDRESSACCTFIELDSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ADDRESSACCOUNTFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSACCTFIELDSKEY), '0'), 38, 10) is null and 
                    src:ADDRESSACCTFIELDSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXACTIVEOWNERS), '0'), 38, 10) is null and 
                    src:MAXACTIVEOWNERS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXACTIVETENANTS), '0'), 38, 10) is null and 
                    src:MAXACTIVETENANTS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)