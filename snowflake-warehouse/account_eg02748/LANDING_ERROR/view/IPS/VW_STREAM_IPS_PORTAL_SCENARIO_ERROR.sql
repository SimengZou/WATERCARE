CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PORTAL_SCENARIO_ERROR AS SELECT src, 'IPS_PORTAL_SCENARIO' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVE), '1900-01-01')) is null and 
                    src:LASTSAVE is not null) THEN
                    'LASTSAVE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTSAVE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIP), '0'), 38, 10) is null and 
                    src:MEMBERSHIP is not null) THEN
                    'MEMBERSHIP contains a non-numeric value : \'' || AS_VARCHAR(src:MEMBERSHIP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJECTAPPLICATION), '0'), 38, 10) is null and 
                    src:PROJECTAPPLICATION is not null) THEN
                    'PROJECTAPPLICATION contains a non-numeric value : \'' || AS_VARCHAR(src:PROJECTAPPLICATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REQRECCOUNT), '0'), 38, 10) is null and 
                    src:REQRECCOUNT is not null) THEN
                    'REQRECCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:REQRECCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCENARIOKEY), '0'), 38, 10) is null and 
                    src:SCENARIOKEY is not null) THEN
                    'SCENARIOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCENARIOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBRECCOUNT), '0'), 38, 10) is null and 
                    src:SUBRECCOUNT is not null) THEN
                    'SUBRECCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:SUBRECCOUNT) || '\'' WHEN 
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
                                    
                src:SCENARIOKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PORTAL_SCENARIO as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVE), '1900-01-01')) is null and 
                    src:LASTSAVE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIP), '0'), 38, 10) is null and 
                    src:MEMBERSHIP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJECTAPPLICATION), '0'), 38, 10) is null and 
                    src:PROJECTAPPLICATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REQRECCOUNT), '0'), 38, 10) is null and 
                    src:REQRECCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCENARIOKEY), '0'), 38, 10) is null and 
                    src:SCENARIOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBRECCOUNT), '0'), 38, 10) is null and 
                    src:SUBRECCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)