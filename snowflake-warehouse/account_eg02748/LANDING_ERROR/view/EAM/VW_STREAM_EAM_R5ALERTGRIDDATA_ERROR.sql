CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTGRIDDATA_ERROR AS SELECT src, 'EAM_R5ALERTGRIDDATA' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGD_DATASPYID), '0'), 38, 10) is null and 
                    src:AGD_DATASPYID is not null) THEN
                    'AGD_DATASPYID contains a non-numeric value : \'' || AS_VARCHAR(src:AGD_DATASPYID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_DATE), '1900-01-01')) is null and 
                    src:AGD_DATE is not null) THEN
                    'AGD_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:AGD_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGD_GRIDID), '0'), 38, 10) is null and 
                    src:AGD_GRIDID is not null) THEN
                    'AGD_GRIDID contains a non-numeric value : \'' || AS_VARCHAR(src:AGD_GRIDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is null and 
                    src:AGD_LASTSAVED is not null) THEN
                    'AGD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AGD_LASTSAVED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is null and 
                src:AGD_LASTSAVED is not null) THEN
                'AGD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AGD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:AGD_PK  ORDER BY 
            src:AGD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTGRIDDATA as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGD_DATASPYID), '0'), 38, 10) is null and 
                    src:AGD_DATASPYID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_DATE), '1900-01-01')) is null and 
                    src:AGD_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGD_GRIDID), '0'), 38, 10) is null and 
                    src:AGD_GRIDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is null and 
                    src:AGD_LASTSAVED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is null and 
                src:AGD_LASTSAVED is not null)