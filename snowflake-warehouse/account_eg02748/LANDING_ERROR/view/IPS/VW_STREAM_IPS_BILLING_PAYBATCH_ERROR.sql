CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYBATCH_ERROR AS SELECT src, 'IPS_BILLING_PAYBATCH' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BATCFRDTTM), '1900-01-01')) is null and 
                    src:BATCFRDTTM is not null) THEN
                    'BATCFRDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:BATCFRDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHAMT), '0'), 38, 10) is null and 
                    src:BATCHAMT is not null) THEN
                    'BATCHAMT contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHCT), '0'), 38, 10) is null and 
                    src:BATCHCT is not null) THEN
                    'BATCHCT contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) THEN
                    'BATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BATCTODTTM), '1900-01-01')) is null and 
                    src:BATCTODTTM is not null) THEN
                    'BATCTODTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:BATCTODTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) THEN
                    'DIRECTDEBITRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:POSTDTTM), '1900-01-01')) is null and 
                    src:POSTDTTM is not null) THEN
                    'POSTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:POSTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VOIDEDDATE), '1900-01-01')) is null and 
                    src:VOIDEDDATE is not null) THEN
                    'VOIDEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:VOIDEDDATE) || '\'' WHEN 
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
                                    
                src:BATCHKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYBATCH as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BATCFRDTTM), '1900-01-01')) is null and 
                    src:BATCFRDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHAMT), '0'), 38, 10) is null and 
                    src:BATCHAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHCT), '0'), 38, 10) is null and 
                    src:BATCHCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BATCTODTTM), '1900-01-01')) is null and 
                    src:BATCTODTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:POSTDTTM), '1900-01-01')) is null and 
                    src:POSTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VOIDEDDATE), '1900-01-01')) is null and 
                    src:VOIDEDDATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)