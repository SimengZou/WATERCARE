CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WARCOVERAGES_ERROR AS SELECT src, 'EAM_R5WARCOVERAGES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_CREATED), '1900-01-01')) is null and 
                    src:WCV_CREATED is not null) THEN
                    'WCV_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_DURATION), '0'), 38, 10) is null and 
                    src:WCV_DURATION is not null) THEN
                    'WCV_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_EXPIRATION), '0'), 38, 10) is null and 
                    src:WCV_EXPIRATION is not null) THEN
                    'WCV_EXPIRATION contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_EXPIRATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_EXPIRATIONDATE), '1900-01-01')) is null and 
                    src:WCV_EXPIRATIONDATE is not null) THEN
                    'WCV_EXPIRATIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_EXPIRATIONDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_LASTSAVED), '1900-01-01')) is null and 
                    src:WCV_LASTSAVED is not null) THEN
                    'WCV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_NEARTHRESHOLD), '0'), 38, 10) is null and 
                    src:WCV_NEARTHRESHOLD is not null) THEN
                    'WCV_NEARTHRESHOLD contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_NEARTHRESHOLD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_RECORDED), '1900-01-01')) is null and 
                    src:WCV_RECORDED is not null) THEN
                    'WCV_RECORDED contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_RECORDED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_SEQNO), '0'), 38, 10) is null and 
                    src:WCV_SEQNO is not null) THEN
                    'WCV_SEQNO contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_SEQNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_STARTDATE), '1900-01-01')) is null and 
                    src:WCV_STARTDATE is not null) THEN
                    'WCV_STARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_STARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_STARTUSAGE), '0'), 38, 10) is null and 
                    src:WCV_STARTUSAGE is not null) THEN
                    'WCV_STARTUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_STARTUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WCV_UPDATECOUNT is not null) THEN
                    'WCV_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WCV_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_UPDATED), '1900-01-01')) is null and 
                    src:WCV_UPDATED is not null) THEN
                    'WCV_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_LASTSAVED), '1900-01-01')) is null and 
                src:WCV_LASTSAVED is not null) THEN
                'WCV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WCV_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WCV_SEQNO  ORDER BY 
            src:WCV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WARCOVERAGES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_CREATED), '1900-01-01')) is null and 
                    src:WCV_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_DURATION), '0'), 38, 10) is null and 
                    src:WCV_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_EXPIRATION), '0'), 38, 10) is null and 
                    src:WCV_EXPIRATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_EXPIRATIONDATE), '1900-01-01')) is null and 
                    src:WCV_EXPIRATIONDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_LASTSAVED), '1900-01-01')) is null and 
                    src:WCV_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_NEARTHRESHOLD), '0'), 38, 10) is null and 
                    src:WCV_NEARTHRESHOLD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_RECORDED), '1900-01-01')) is null and 
                    src:WCV_RECORDED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_SEQNO), '0'), 38, 10) is null and 
                    src:WCV_SEQNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_STARTDATE), '1900-01-01')) is null and 
                    src:WCV_STARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_STARTUSAGE), '0'), 38, 10) is null and 
                    src:WCV_STARTUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WCV_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_UPDATED), '1900-01-01')) is null and 
                    src:WCV_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCV_LASTSAVED), '1900-01-01')) is null and 
                src:WCV_LASTSAVED is not null)