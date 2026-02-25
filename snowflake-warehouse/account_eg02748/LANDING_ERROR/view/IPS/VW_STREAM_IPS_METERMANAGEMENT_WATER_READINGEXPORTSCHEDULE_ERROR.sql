CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSCHEDULE_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGEXPORTSCHEDULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY), '0'), 38, 10) is null and 
                    src:DAYSBEFORENEXTREADINGDAY is not null) THEN
                    'DAYSBEFORENEXTREADINGDAY contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLBILLEDDAYS), '0'), 38, 10) is null and 
                    src:EXCLBILLEDDAYS is not null) THEN
                    'EXCLBILLEDDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLBILLEDDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLLASTREADDAYSAGO), '0'), 38, 10) is null and 
                    src:EXCLLASTREADDAYSAGO is not null) THEN
                    'EXCLLASTREADDAYSAGO contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLLASTREADDAYSAGO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRAFTERDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRAFTERDAYS is not null) THEN
                    'EXCLSRAFTERDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLSRAFTERDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRBEFOREDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRBEFOREDAYS is not null) THEN
                    'EXCLSRBEFOREDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLSRBEFOREDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPORTDATE), '1900-01-01')) is null and 
                    src:EXPORTDATE is not null) THEN
                    'EXPORTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPORTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:EXPORTRUNNUMBER is not null) THEN
                    'EXPORTRUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:EXPORTRUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPE), '0'), 38, 10) is null and 
                    src:EXPORTTYPE is not null) THEN
                    'EXPORTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPORTTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTREADINGDATE), '1900-01-01')) is null and 
                    src:NEXTREADINGDATE is not null) THEN
                    'NEXTREADINGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTREADINGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTSCHEDULEKEY is not null) THEN
                    'READINGEXPORTSCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGEXPORTSCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is null and 
                    src:RERUNNUMBER is not null) THEN
                    'RERUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RERUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) THEN
                    'RUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
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
                                    
                src:READINGEXPORTSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY), '0'), 38, 10) is null and 
                    src:DAYSBEFORENEXTREADINGDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLBILLEDDAYS), '0'), 38, 10) is null and 
                    src:EXCLBILLEDDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLLASTREADDAYSAGO), '0'), 38, 10) is null and 
                    src:EXCLLASTREADDAYSAGO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRAFTERDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRAFTERDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRBEFOREDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRBEFOREDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPORTDATE), '1900-01-01')) is null and 
                    src:EXPORTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:EXPORTRUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPE), '0'), 38, 10) is null and 
                    src:EXPORTTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTREADINGDATE), '1900-01-01')) is null and 
                    src:NEXTREADINGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTSCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is null and 
                    src:RERUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)