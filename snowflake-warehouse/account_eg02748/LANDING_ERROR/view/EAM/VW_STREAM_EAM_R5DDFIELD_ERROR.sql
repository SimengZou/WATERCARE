CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DDFIELD_ERROR AS SELECT src, 'EAM_R5DDFIELD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_FIELDID), '0'), 38, 10) is null and 
                    src:DDF_FIELDID is not null) THEN
                    'DDF_FIELDID contains a non-numeric value : \'' || AS_VARCHAR(src:DDF_FIELDID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDF_LASTSAVED), '1900-01-01')) is null and 
                    src:DDF_LASTSAVED is not null) THEN
                    'DDF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DDF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DDF_UPDATECOUNT is not null) THEN
                    'DDF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DDF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_VALUEMAPID), '0'), 38, 10) is null and 
                    src:DDF_VALUEMAPID is not null) THEN
                    'DDF_VALUEMAPID contains a non-numeric value : \'' || AS_VARCHAR(src:DDF_VALUEMAPID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDF_LASTSAVED), '1900-01-01')) is null and 
                src:DDF_LASTSAVED is not null) THEN
                'DDF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DDF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DDF_FIELDID  ORDER BY 
            src:DDF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DDFIELD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_FIELDID), '0'), 38, 10) is null and 
                    src:DDF_FIELDID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDF_LASTSAVED), '1900-01-01')) is null and 
                    src:DDF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DDF_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DDF_VALUEMAPID), '0'), 38, 10) is null and 
                    src:DDF_VALUEMAPID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DDF_LASTSAVED), '1900-01-01')) is null and 
                src:DDF_LASTSAVED is not null)