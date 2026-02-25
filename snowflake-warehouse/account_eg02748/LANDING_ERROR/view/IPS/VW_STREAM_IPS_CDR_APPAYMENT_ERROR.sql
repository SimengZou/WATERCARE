CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_APPAYMENT_ERROR AS SELECT src, 'IPS_CDR_APPAYMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is null and 
                    src:AMTCONF is not null) THEN
                    'AMTCONF contains a non-numeric value : \'' || AS_VARCHAR(src:AMTCONF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) THEN
                    'APPAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPAYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPDT), '1900-01-01')) is null and 
                    src:CARDEXPDT is not null) THEN
                    'CARDEXPDT contains a non-timestamp value : \'' || AS_VARCHAR(src:CARDEXPDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is null and 
                    src:CONFDTTM is not null) THEN
                    'CONFDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CONFDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCTKEY is not null) THEN
                    'ESCROWACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWACCTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is null and 
                    src:ESCROWTRNNO is not null) THEN
                    'ESCROWTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWTRNNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMT), '0'), 38, 10) is null and 
                    src:PAYAMT is not null) THEN
                    'PAYAMT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAYDTTM), '1900-01-01')) is null and 
                    src:PAYDTTM is not null) THEN
                    'PAYDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:PAYDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTDISTRIBUTION), '0'), 38, 10) is null and 
                    src:PAYMENTDISTRIBUTION is not null) THEN
                    'PAYMENTDISTRIBUTION contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTDISTRIBUTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTMETHOD), '0'), 38, 10) is null and 
                    src:PAYMENTMETHOD is not null) THEN
                    'PAYMENTMETHOD contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTMETHOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) THEN
                    'REGTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REGTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) THEN
                    'SRCBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SRCBGTNOKEY) || '\'' WHEN 
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
                                    
                src:APPAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APPAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is null and 
                    src:AMTCONF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPAYKEY), '0'), 38, 10) is null and 
                    src:APPAYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPDT), '1900-01-01')) is null and 
                    src:CARDEXPDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is null and 
                    src:CONFDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is null and 
                    src:ESCROWTRNNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMT), '0'), 38, 10) is null and 
                    src:PAYAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAYDTTM), '1900-01-01')) is null and 
                    src:PAYDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTDISTRIBUTION), '0'), 38, 10) is null and 
                    src:PAYMENTDISTRIBUTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTMETHOD), '0'), 38, 10) is null and 
                    src:PAYMENTMETHOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is null and 
                    src:SRCBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)