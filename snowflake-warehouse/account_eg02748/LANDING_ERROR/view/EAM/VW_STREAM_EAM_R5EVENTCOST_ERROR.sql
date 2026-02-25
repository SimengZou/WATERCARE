CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EVENTCOST_ERROR AS SELECT src, 'EAM_R5EVENTCOST' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_DIRECTMATERIAL), '0'), 38, 10) is null and 
                    src:EVO_DIRECTMATERIAL is not null) THEN
                    'EVO_DIRECTMATERIAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_DIRECTMATERIAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_HIREDLABOR), '0'), 38, 10) is null and 
                    src:EVO_HIREDLABOR is not null) THEN
                    'EVO_HIREDLABOR contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_HIREDLABOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_HOURS), '0'), 38, 10) is null and 
                    src:EVO_HOURS is not null) THEN
                    'EVO_HOURS contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_HOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_LABOR), '0'), 38, 10) is null and 
                    src:EVO_LABOR is not null) THEN
                    'EVO_LABOR contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_LABOR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_LASTSAVED), '1900-01-01')) is null and 
                    src:EVO_LASTSAVED is not null) THEN
                    'EVO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_MATERIAL), '0'), 38, 10) is null and 
                    src:EVO_MATERIAL is not null) THEN
                    'EVO_MATERIAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_MATERIAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_OWNLABOR), '0'), 38, 10) is null and 
                    src:EVO_OWNLABOR is not null) THEN
                    'EVO_OWNLABOR contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_OWNLABOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_SERVICESLABOR), '0'), 38, 10) is null and 
                    src:EVO_SERVICESLABOR is not null) THEN
                    'EVO_SERVICESLABOR contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_SERVICESLABOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_STOCKMATERIAL), '0'), 38, 10) is null and 
                    src:EVO_STOCKMATERIAL is not null) THEN
                    'EVO_STOCKMATERIAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_STOCKMATERIAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_TOOL), '0'), 38, 10) is null and 
                    src:EVO_TOOL is not null) THEN
                    'EVO_TOOL contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_TOOL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_TOTAL), '0'), 38, 10) is null and 
                    src:EVO_TOTAL is not null) THEN
                    'EVO_TOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:EVO_TOTAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_UPDATED), '1900-01-01')) is null and 
                    src:EVO_UPDATED is not null) THEN
                    'EVO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVO_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_LASTSAVED), '1900-01-01')) is null and 
                src:EVO_LASTSAVED is not null) THEN
                'EVO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:EVO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:EVO_EVENT  ORDER BY 
            src:EVO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EVENTCOST as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_DIRECTMATERIAL), '0'), 38, 10) is null and 
                    src:EVO_DIRECTMATERIAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_HIREDLABOR), '0'), 38, 10) is null and 
                    src:EVO_HIREDLABOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_HOURS), '0'), 38, 10) is null and 
                    src:EVO_HOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_LABOR), '0'), 38, 10) is null and 
                    src:EVO_LABOR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_LASTSAVED), '1900-01-01')) is null and 
                    src:EVO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_MATERIAL), '0'), 38, 10) is null and 
                    src:EVO_MATERIAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_OWNLABOR), '0'), 38, 10) is null and 
                    src:EVO_OWNLABOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_SERVICESLABOR), '0'), 38, 10) is null and 
                    src:EVO_SERVICESLABOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_STOCKMATERIAL), '0'), 38, 10) is null and 
                    src:EVO_STOCKMATERIAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_TOOL), '0'), 38, 10) is null and 
                    src:EVO_TOOL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EVO_TOTAL), '0'), 38, 10) is null and 
                    src:EVO_TOTAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_UPDATED), '1900-01-01')) is null and 
                    src:EVO_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EVO_LASTSAVED), '1900-01-01')) is null and 
                src:EVO_LASTSAVED is not null)