CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PROPERTYVALUES_ERROR AS SELECT src, 'EAM_R5PROPERTYVALUES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_CREATED), '1900-01-01')) is null and 
                    src:PRV_CREATED is not null) THEN
                    'PRV_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRV_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_DVALUE), '1900-01-01')) is null and 
                    src:PRV_DVALUE is not null) THEN
                    'PRV_DVALUE contains a non-timestamp value : \'' || AS_VARCHAR(src:PRV_DVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is null and 
                    src:PRV_LASTSAVED is not null) THEN
                    'PRV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRV_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_NVALUE), '0'), 38, 10) is null and 
                    src:PRV_NVALUE is not null) THEN
                    'PRV_NVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:PRV_NVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_SEQNO), '0'), 38, 10) is null and 
                    src:PRV_SEQNO is not null) THEN
                    'PRV_SEQNO contains a non-numeric value : \'' || AS_VARCHAR(src:PRV_SEQNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRV_UPDATECOUNT is not null) THEN
                    'PRV_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PRV_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_UPDATED), '1900-01-01')) is null and 
                    src:PRV_UPDATED is not null) THEN
                    'PRV_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRV_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is null and 
                src:PRV_LASTSAVED is not null) THEN
                'PRV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRV_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PRV_CLASS,
                src:PRV_CLASS_ORG,
                src:PRV_CODE,
                src:PRV_PROPERTY,
                src:PRV_RENTITY,
                src:PRV_SEQNO  ORDER BY 
            src:PRV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROPERTYVALUES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_CREATED), '1900-01-01')) is null and 
                    src:PRV_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_DVALUE), '1900-01-01')) is null and 
                    src:PRV_DVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is null and 
                    src:PRV_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_NVALUE), '0'), 38, 10) is null and 
                    src:PRV_NVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_SEQNO), '0'), 38, 10) is null and 
                    src:PRV_SEQNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRV_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_UPDATED), '1900-01-01')) is null and 
                    src:PRV_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is null and 
                src:PRV_LASTSAVED is not null)