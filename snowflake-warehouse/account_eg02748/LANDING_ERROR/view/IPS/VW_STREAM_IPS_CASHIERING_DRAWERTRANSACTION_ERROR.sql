CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_DRAWERTRANSACTION_ERROR AS SELECT src, 'IPS_CASHIERING_DRAWERTRANSACTION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) THEN
                    'BGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:BGTNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXP), '1900-01-01')) is null and 
                    src:CARDEXP is not null) THEN
                    'CARDEXP contains a non-timestamp value : \'' || AS_VARCHAR(src:CARDEXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRKEY), '0'), 38, 10) is null and 
                    src:DRWRKEY is not null) THEN
                    'DRWRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DRWRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRTRANNO), '0'), 38, 10) is null and 
                    src:DRWRTRANNO is not null) THEN
                    'DRWRTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:DRWRTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCTKEY is not null) THEN
                    'ESCROWACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWACCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSERVERTRAN), '0'), 38, 10) is null and 
                    src:PAYSERVERTRAN is not null) THEN
                    'PAYSERVERTRAN contains a non-numeric value : \'' || AS_VARCHAR(src:PAYSERVERTRAN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRANNO), '0'), 38, 10) is null and 
                    src:REFTRANNO is not null) THEN
                    'REFTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REFTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is null and 
                    src:REGKEY is not null) THEN
                    'REGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) THEN
                    'REGTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REGTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANAMT), '0'), 38, 10) is null and 
                    src:TRANAMT is not null) THEN
                    'TRANAMT contains a non-numeric value : \'' || AS_VARCHAR(src:TRANAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANDTTM), '1900-01-01')) is null and 
                    src:TRANDTTM is not null) THEN
                    'TRANDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VOIDDTTM), '1900-01-01')) is null and 
                    src:VOIDDTTM is not null) THEN
                    'VOIDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:VOIDDTTM) || '\'' WHEN 
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
                                    
                src:DRWRTRANNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_DRAWERTRANSACTION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXP), '1900-01-01')) is null and 
                    src:CARDEXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRKEY), '0'), 38, 10) is null and 
                    src:DRWRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRTRANNO), '0'), 38, 10) is null and 
                    src:DRWRTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSERVERTRAN), '0'), 38, 10) is null and 
                    src:PAYSERVERTRAN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRANNO), '0'), 38, 10) is null and 
                    src:REFTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGKEY), '0'), 38, 10) is null and 
                    src:REGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANAMT), '0'), 38, 10) is null and 
                    src:TRANAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANDTTM), '1900-01-01')) is null and 
                    src:TRANDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VOIDDTTM), '1900-01-01')) is null and 
                    src:VOIDDTTM is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)