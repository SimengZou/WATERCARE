CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_FIXCHGSU_ERROR AS SELECT src, 'IPS_BILLING_FIXCHGSU' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE1), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE1 is not null) THEN
                    'BILLEDTHROUGHDATE1 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE10), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE10 is not null) THEN
                    'BILLEDTHROUGHDATE10 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE10) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE11), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE11 is not null) THEN
                    'BILLEDTHROUGHDATE11 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE11) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE12), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE12 is not null) THEN
                    'BILLEDTHROUGHDATE12 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE12) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE2), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE2 is not null) THEN
                    'BILLEDTHROUGHDATE2 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE3), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE3 is not null) THEN
                    'BILLEDTHROUGHDATE3 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE4), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE4 is not null) THEN
                    'BILLEDTHROUGHDATE4 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE4) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE5), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE5 is not null) THEN
                    'BILLEDTHROUGHDATE5 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE5) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE6), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE6 is not null) THEN
                    'BILLEDTHROUGHDATE6 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE6) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE7), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE7 is not null) THEN
                    'BILLEDTHROUGHDATE7 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE7) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE8), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE8 is not null) THEN
                    'BILLEDTHROUGHDATE8 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE8) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE9), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE9 is not null) THEN
                    'BILLEDTHROUGHDATE9 contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLEDTHROUGHDATE9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXCHGSUKEY), '0'), 38, 10) is null and 
                    src:FIXCHGSUKEY is not null) THEN
                    'FIXCHGSUKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FIXCHGSUKEY) || '\'' WHEN 
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
                                    
                src:FIXCHGSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_FIXCHGSU as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE1), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE10), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE10 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE11), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE11 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE12), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE12 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE2), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE3), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE4), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE4 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE5), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE5 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE6), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE6 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE7), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE7 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE8), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE8 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLEDTHROUGHDATE9), '1900-01-01')) is null and 
                    src:BILLEDTHROUGHDATE9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FIXCHGSUKEY), '0'), 38, 10) is null and 
                    src:FIXCHGSUKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)