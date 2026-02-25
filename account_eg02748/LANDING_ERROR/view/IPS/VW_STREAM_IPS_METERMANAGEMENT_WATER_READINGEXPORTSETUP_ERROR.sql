CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY), '0'), 38, 10) is null and 
                    src:DAYSBEFORENEXTREADINGDAY is not null) THEN
                    'DAYSBEFORENEXTREADINGDAY contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLBILLEDDAYS), '0'), 38, 10) is null and 
                    src:EXCLBILLEDDAYS is not null) THEN
                    'EXCLBILLEDDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLBILLEDDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLLASTREADDAYS), '0'), 38, 10) is null and 
                    src:EXCLLASTREADDAYS is not null) THEN
                    'EXCLLASTREADDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLLASTREADDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRAFTERDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRAFTERDAYS is not null) THEN
                    'EXCLSRAFTERDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLSRAFTERDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRBEFOREDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRBEFOREDAYS is not null) THEN
                    'EXCLSRBEFOREDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:EXCLSRBEFOREDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:EXPORTRUNNUMBER is not null) THEN
                    'EXPORTRUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:EXPORTRUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPE), '0'), 38, 10) is null and 
                    src:EXPORTTYPE is not null) THEN
                    'EXPORTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPORTTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FILENUMBERCOUNTER), '0'), 38, 10) is null and 
                    src:FILENUMBERCOUNTER is not null) THEN
                    'FILENUMBERCOUNTER contains a non-numeric value : \'' || AS_VARCHAR(src:FILENUMBERCOUNTER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTSETUPKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTSETUPKEY is not null) THEN
                    'READINGEXPORTSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGEXPORTSETUPKEY) || '\'' WHEN 
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
                                    
                src:READINGEXPORTSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSBEFORENEXTREADINGDAY), '0'), 38, 10) is null and 
                    src:DAYSBEFORENEXTREADINGDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLBILLEDDAYS), '0'), 38, 10) is null and 
                    src:EXCLBILLEDDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLLASTREADDAYS), '0'), 38, 10) is null and 
                    src:EXCLLASTREADDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRAFTERDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRAFTERDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCLSRBEFOREDAYS), '0'), 38, 10) is null and 
                    src:EXCLSRBEFOREDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:EXPORTRUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPORTTYPE), '0'), 38, 10) is null and 
                    src:EXPORTTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FILENUMBERCOUNTER), '0'), 38, 10) is null and 
                    src:FILENUMBERCOUNTER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTSETUPKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)