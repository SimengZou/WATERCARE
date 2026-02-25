CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_ESCROWPAYMENT_ERROR AS SELECT src, 'IPS_RESOURCES_ESCROWPAYMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPDT), '1900-01-01')) is null and 
                    src:CARDEXPDT is not null) THEN
                    'CARDEXPDT contains a non-timestamp value : \'' || AS_VARCHAR(src:CARDEXPDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWPAYKEY), '0'), 38, 10) is null and 
                    src:ESCROWPAYKEY is not null) THEN
                    'ESCROWPAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWPAYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMT), '0'), 38, 10) is null and 
                    src:PAYAMT is not null) THEN
                    'PAYAMT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAYDTTM), '1900-01-01')) is null and 
                    src:PAYDTTM is not null) THEN
                    'PAYDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:PAYDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMETHOD), '0'), 38, 10) is null and 
                    src:PAYMETHOD is not null) THEN
                    'PAYMETHOD contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMETHOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRNNO), '0'), 38, 10) is null and 
                    src:REGTRNNO is not null) THEN
                    'REGTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:REGTRNNO) || '\'' WHEN 
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
                                    
                src:ESCROWPAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_ESCROWPAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPDT), '1900-01-01')) is null and 
                    src:CARDEXPDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWPAYKEY), '0'), 38, 10) is null and 
                    src:ESCROWPAYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMT), '0'), 38, 10) is null and 
                    src:PAYAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAYDTTM), '1900-01-01')) is null and 
                    src:PAYDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMETHOD), '0'), 38, 10) is null and 
                    src:PAYMETHOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRNNO), '0'), 38, 10) is null and 
                    src:REGTRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)