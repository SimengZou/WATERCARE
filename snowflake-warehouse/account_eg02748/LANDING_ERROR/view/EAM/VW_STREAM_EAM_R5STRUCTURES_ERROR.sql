CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5STRUCTURES_ERROR AS SELECT src, 'EAM_R5STRUCTURES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_LASTSAVED), '1900-01-01')) is null and 
                    src:STC_LASTSAVED is not null) THEN
                    'STC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STC_SEQUENCE), '0'), 38, 10) is null and 
                    src:STC_SEQUENCE is not null) THEN
                    'STC_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:STC_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STC_UPDATECOUNT is not null) THEN
                    'STC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:STC_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_UPDATED), '1900-01-01')) is null and 
                    src:STC_UPDATED is not null) THEN
                    'STC_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STC_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_LASTSAVED), '1900-01-01')) is null and 
                src:STC_LASTSAVED is not null) THEN
                'STC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:STC_CHILD,
                src:STC_CHILD_ORG,
                src:STC_PARENT,
                src:STC_PARENT_ORG  ORDER BY 
            src:STC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STRUCTURES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_LASTSAVED), '1900-01-01')) is null and 
                    src:STC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STC_SEQUENCE), '0'), 38, 10) is null and 
                    src:STC_SEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STC_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_UPDATED), '1900-01-01')) is null and 
                    src:STC_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STC_LASTSAVED), '1900-01-01')) is null and 
                src:STC_LASTSAVED is not null)