CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_CYCLEROUTEAVGCONSUMPTIONRUN_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_CYCLEROUTEAVGCONSUMPTIONRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEROUTEAVGCONSKEY), '0'), 38, 10) is null and 
                    src:CYCLEROUTEAVGCONSKEY is not null) THEN
                    'CYCLEROUTEAVGCONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CYCLEROUTEAVGCONSKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PERIODFROM), '1900-01-01')) is null and 
                    src:PERIODFROM is not null) THEN
                    'PERIODFROM contains a non-timestamp value : \'' || AS_VARCHAR(src:PERIODFROM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PERIODTO), '1900-01-01')) is null and 
                    src:PERIODTO is not null) THEN
                    'PERIODTO contains a non-timestamp value : \'' || AS_VARCHAR(src:PERIODTO) || '\'' WHEN 
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
                                    
                src:CYCLEROUTEAVGCONSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_CYCLEROUTEAVGCONSUMPTIONRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEROUTEAVGCONSKEY), '0'), 38, 10) is null and 
                    src:CYCLEROUTEAVGCONSKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PERIODFROM), '1900-01-01')) is null and 
                    src:PERIODFROM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PERIODTO), '1900-01-01')) is null and 
                    src:PERIODTO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)