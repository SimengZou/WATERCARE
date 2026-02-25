CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTACTIVITY_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGIMPORTACTIVITY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:IMPORTRUNNUMBER is not null) THEN
                    'IMPORTRUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:IMPORTRUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPORTTYPE), '0'), 38, 10) is null and 
                    src:IMPORTTYPE is not null) THEN
                    'IMPORTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:IMPORTTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFMETERS), '0'), 38, 10) is null and 
                    src:NUMBEROFMETERS is not null) THEN
                    'NUMBEROFMETERS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFMETERS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTACTIVITYKEY is not null) THEN
                    'READINGEXPORTACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGEXPORTACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITYKEY is not null) THEN
                    'READINGIMPORTACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTSCHEDULEKEY is not null) THEN
                    'READINGIMPORTSCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGIMPORTSCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is null and 
                    src:RERUNNUMBER is not null) THEN
                    'RERUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RERUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) THEN
                    'STATUS contains a non-numeric value : \'' || AS_VARCHAR(src:STATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) THEN
                    'STOPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STOPDTTM) || '\'' WHEN 
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
                                    
                src:READINGIMPORTACTIVITYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGIMPORTACTIVITY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPORTRUNNUMBER), '0'), 38, 10) is null and 
                    src:IMPORTRUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPORTTYPE), '0'), 38, 10) is null and 
                    src:IMPORTTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFMETERS), '0'), 38, 10) is null and 
                    src:NUMBEROFMETERS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGEXPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGEXPORTACTIVITYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTACTIVITYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGIMPORTSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:READINGIMPORTSCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RERUNNUMBER), '0'), 38, 10) is null and 
                    src:RERUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STATUS), '0'), 38, 10) is null and 
                    src:STATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)