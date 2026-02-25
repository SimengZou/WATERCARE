CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CATALOGUE_ERROR AS SELECT src, 'EAM_R5CATALOGUE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_DATE), '1900-01-01')) is null and 
                    src:CAT_DATE is not null) THEN
                    'CAT_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCH), '0'), 38, 10) is null and 
                    src:CAT_EXCH is not null) THEN
                    'CAT_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_EXCH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:CAT_EXCHFROMDUAL is not null) THEN
                    'CAT_EXCHFROMDUAL contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_EXCHFROMDUAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:CAT_EXCHTODUAL is not null) THEN
                    'CAT_EXCHTODUAL contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_EXCHTODUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_EXPPURPR), '1900-01-01')) is null and 
                    src:CAT_EXPPURPR is not null) THEN
                    'CAT_EXPPURPR contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_EXPPURPR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_EXPQUOT), '1900-01-01')) is null and 
                    src:CAT_EXPQUOT is not null) THEN
                    'CAT_EXPQUOT contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_EXPQUOT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_GROSS), '0'), 38, 10) is null and 
                    src:CAT_GROSS is not null) THEN
                    'CAT_GROSS contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_GROSS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_IPERROR), '0'), 38, 10) is null and 
                    src:CAT_IPERROR is not null) THEN
                    'CAT_IPERROR contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_IPERROR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_IPLASTUPDATE), '1900-01-01')) is null and 
                    src:CAT_IPLASTUPDATE is not null) THEN
                    'CAT_IPLASTUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_IPLASTUPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_LASTSAVED), '1900-01-01')) is null and 
                    src:CAT_LASTSAVED is not null) THEN
                    'CAT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_LEADTIME), '0'), 38, 10) is null and 
                    src:CAT_LEADTIME is not null) THEN
                    'CAT_LEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_LEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_MINORDQTY), '0'), 38, 10) is null and 
                    src:CAT_MINORDQTY is not null) THEN
                    'CAT_MINORDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_MINORDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_MULTIPLY), '0'), 38, 10) is null and 
                    src:CAT_MULTIPLY is not null) THEN
                    'CAT_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_PURPRICE), '0'), 38, 10) is null and 
                    src:CAT_PURPRICE is not null) THEN
                    'CAT_PURPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_PURPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_QUOTPRICE), '0'), 38, 10) is null and 
                    src:CAT_QUOTPRICE is not null) THEN
                    'CAT_QUOTPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_QUOTPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_REPPRICE), '0'), 38, 10) is null and 
                    src:CAT_REPPRICE is not null) THEN
                    'CAT_REPPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_REPPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CAT_UPDATECOUNT is not null) THEN
                    'CAT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CAT_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_LASTSAVED), '1900-01-01')) is null and 
                src:CAT_LASTSAVED is not null) THEN
                'CAT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CAT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CAT_PART,
                src:CAT_PART_ORG,
                src:CAT_SUPPLIER,
                src:CAT_SUPPLIER_ORG  ORDER BY 
            src:CAT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CATALOGUE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_DATE), '1900-01-01')) is null and 
                    src:CAT_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCH), '0'), 38, 10) is null and 
                    src:CAT_EXCH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:CAT_EXCHFROMDUAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:CAT_EXCHTODUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_EXPPURPR), '1900-01-01')) is null and 
                    src:CAT_EXPPURPR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_EXPQUOT), '1900-01-01')) is null and 
                    src:CAT_EXPQUOT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_GROSS), '0'), 38, 10) is null and 
                    src:CAT_GROSS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_IPERROR), '0'), 38, 10) is null and 
                    src:CAT_IPERROR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_IPLASTUPDATE), '1900-01-01')) is null and 
                    src:CAT_IPLASTUPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_LASTSAVED), '1900-01-01')) is null and 
                    src:CAT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_LEADTIME), '0'), 38, 10) is null and 
                    src:CAT_LEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_MINORDQTY), '0'), 38, 10) is null and 
                    src:CAT_MINORDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_MULTIPLY), '0'), 38, 10) is null and 
                    src:CAT_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_PURPRICE), '0'), 38, 10) is null and 
                    src:CAT_PURPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_QUOTPRICE), '0'), 38, 10) is null and 
                    src:CAT_QUOTPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_REPPRICE), '0'), 38, 10) is null and 
                    src:CAT_REPPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CAT_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAT_LASTSAVED), '1900-01-01')) is null and 
                src:CAT_LASTSAVED is not null)