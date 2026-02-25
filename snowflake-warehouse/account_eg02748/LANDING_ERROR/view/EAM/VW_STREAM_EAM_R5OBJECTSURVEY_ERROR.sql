CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5OBJECTSURVEY_ERROR AS SELECT src, 'EAM_R5OBJECTSURVEY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_CALCULATEDVALUE), '0'), 38, 10) is null and 
                    src:OBS_CALCULATEDVALUE is not null) THEN
                    'OBS_CALCULATEDVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OBS_CALCULATEDVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_DATEEFFECTIVE), '1900-01-01')) is null and 
                    src:OBS_DATEEFFECTIVE is not null) THEN
                    'OBS_DATEEFFECTIVE contains a non-timestamp value : \'' || AS_VARCHAR(src:OBS_DATEEFFECTIVE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_DATELASTCALCULATED), '1900-01-01')) is null and 
                    src:OBS_DATELASTCALCULATED is not null) THEN
                    'OBS_DATELASTCALCULATED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBS_DATELASTCALCULATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is null and 
                    src:OBS_LASTSAVED is not null) THEN
                    'OBS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBS_UPDATECOUNT is not null) THEN
                    'OBS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OBS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_VALUE), '0'), 38, 10) is null and 
                    src:OBS_VALUE is not null) THEN
                    'OBS_VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OBS_VALUE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is null and 
                src:OBS_LASTSAVED is not null) THEN
                'OBS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OBS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:OBS_LEVELPK,
                src:OBS_OBJECT,
                src:OBS_OBJECT_ORG,
                src:OBS_TYPE  ORDER BY 
            src:OBS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5OBJECTSURVEY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_CALCULATEDVALUE), '0'), 38, 10) is null and 
                    src:OBS_CALCULATEDVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_DATEEFFECTIVE), '1900-01-01')) is null and 
                    src:OBS_DATEEFFECTIVE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_DATELASTCALCULATED), '1900-01-01')) is null and 
                    src:OBS_DATELASTCALCULATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is null and 
                    src:OBS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OBS_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OBS_VALUE), '0'), 38, 10) is null and 
                    src:OBS_VALUE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is null and 
                src:OBS_LASTSAVED is not null)