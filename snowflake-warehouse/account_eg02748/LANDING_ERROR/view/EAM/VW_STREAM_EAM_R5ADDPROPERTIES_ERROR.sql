CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ADDPROPERTIES_ERROR AS SELECT src, 'EAM_R5ADDPROPERTIES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_CREATED), '1900-01-01')) is null and 
                    src:APR_CREATED is not null) THEN
                    'APR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:APR_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is null and 
                    src:APR_LASTSAVED is not null) THEN
                    'APR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:APR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APR_LINE), '0'), 38, 10) is null and 
                    src:APR_LINE is not null) THEN
                    'APR_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:APR_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:APR_UPDATECOUNT is not null) THEN
                    'APR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:APR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_UPDATED), '1900-01-01')) is null and 
                    src:APR_UPDATED is not null) THEN
                    'APR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:APR_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is null and 
                src:APR_LASTSAVED is not null) THEN
                'APR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:APR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:APR_CLASS,
                src:APR_CLASS_ORG,
                src:APR_PROPERTY,
                src:APR_RENTITY  ORDER BY 
            src:APR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ADDPROPERTIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_CREATED), '1900-01-01')) is null and 
                    src:APR_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is null and 
                    src:APR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APR_LINE), '0'), 38, 10) is null and 
                    src:APR_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:APR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_UPDATED), '1900-01-01')) is null and 
                    src:APR_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is null and 
                src:APR_LASTSAVED is not null)