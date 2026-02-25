CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGRANKS_ERROR AS SELECT src, 'EAM_R5RELIABILITYRANKINGRANKS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRR_LASTSAVED), '1900-01-01')) is null and 
                    src:RRR_LASTSAVED is not null) THEN
                    'RRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_MAXVALUE), '0'), 38, 10) is null and 
                    src:RRR_MAXVALUE is not null) THEN
                    'RRR_MAXVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:RRR_MAXVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_MINVALUE), '0'), 38, 10) is null and 
                    src:RRR_MINVALUE is not null) THEN
                    'RRR_MINVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:RRR_MINVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_REVISION), '0'), 38, 10) is null and 
                    src:RRR_REVISION is not null) THEN
                    'RRR_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:RRR_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRR_UPDATECOUNT is not null) THEN
                    'RRR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RRR_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRR_LASTSAVED), '1900-01-01')) is null and 
                src:RRR_LASTSAVED is not null) THEN
                'RRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RRR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RRR_PK  ORDER BY 
            src:RRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGRANKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRR_LASTSAVED), '1900-01-01')) is null and 
                    src:RRR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_MAXVALUE), '0'), 38, 10) is null and 
                    src:RRR_MAXVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_MINVALUE), '0'), 38, 10) is null and 
                    src:RRR_MINVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_REVISION), '0'), 38, 10) is null and 
                    src:RRR_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RRR_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RRR_LASTSAVED), '1900-01-01')) is null and 
                src:RRR_LASTSAVED is not null)