CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTGRIDPARAMS_ERROR AS SELECT src, 'EAM_R5ALERTGRIDPARAMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_DVALUE), '1900-01-01')) is null and 
                    src:AGP_DVALUE is not null) THEN
                    'AGP_DVALUE contains a non-timestamp value : \'' || AS_VARCHAR(src:AGP_DVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is null and 
                    src:AGP_LASTSAVED is not null) THEN
                    'AGP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AGP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGP_NVALUE), '0'), 38, 10) is null and 
                    src:AGP_NVALUE is not null) THEN
                    'AGP_NVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:AGP_NVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AGP_UPDATECOUNT is not null) THEN
                    'AGP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AGP_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is null and 
                src:AGP_LASTSAVED is not null) THEN
                'AGP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AGP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:AGP_ALERT,
                src:AGP_PARAM  ORDER BY 
            src:AGP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTGRIDPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_DVALUE), '1900-01-01')) is null and 
                    src:AGP_DVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is null and 
                    src:AGP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGP_NVALUE), '0'), 38, 10) is null and 
                    src:AGP_NVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AGP_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is null and 
                src:AGP_LASTSAVED is not null)