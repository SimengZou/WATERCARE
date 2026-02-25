CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DOCKLINES_ERROR AS SELECT src, 'EAM_R5DOCKLINES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_ACD), '0'), 38, 10) is null and 
                    src:DKL_ACD is not null) THEN
                    'DKL_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_COUNTQTY), '0'), 38, 10) is null and 
                    src:DKL_COUNTQTY is not null) THEN
                    'DKL_COUNTQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_COUNTQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_INSPDATE), '1900-01-01')) is null and 
                    src:DKL_INSPDATE is not null) THEN
                    'DKL_INSPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_INSPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_INSPECTEDQTY), '0'), 38, 10) is null and 
                    src:DKL_INSPECTEDQTY is not null) THEN
                    'DKL_INSPECTEDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_INSPECTEDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_INSPREJQTY), '0'), 38, 10) is null and 
                    src:DKL_INSPREJQTY is not null) THEN
                    'DKL_INSPREJQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_INSPREJQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is null and 
                    src:DKL_LASTSAVED is not null) THEN
                    'DKL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_LINE), '0'), 38, 10) is null and 
                    src:DKL_LINE is not null) THEN
                    'DKL_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_LINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_MANLOTEXP), '1900-01-01')) is null and 
                    src:DKL_MANLOTEXP is not null) THEN
                    'DKL_MANLOTEXP contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_MANLOTEXP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_MOBILEDATEUPDATED), '1900-01-01')) is null and 
                    src:DKL_MOBILEDATEUPDATED is not null) THEN
                    'DKL_MOBILEDATEUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_MOBILEDATEUPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_ORDLINE), '0'), 38, 10) is null and 
                    src:DKL_ORDLINE is not null) THEN
                    'DKL_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_PRINT), '0'), 38, 10) is null and 
                    src:DKL_PRINT is not null) THEN
                    'DKL_PRINT contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_PRINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_RECVQTY), '0'), 38, 10) is null and 
                    src:DKL_RECVQTY is not null) THEN
                    'DKL_RECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_RECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_REPAIRPRICE), '0'), 38, 10) is null and 
                    src:DKL_REPAIRPRICE is not null) THEN
                    'DKL_REPAIRPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_REPAIRPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_RETNQTY), '0'), 38, 10) is null and 
                    src:DKL_RETNQTY is not null) THEN
                    'DKL_RETNQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_RETNQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:DKL_SCRAPQTY is not null) THEN
                    'DKL_SCRAPQTY contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_SCRAPQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE01), '1900-01-01')) is null and 
                    src:DKL_UDFDATE01 is not null) THEN
                    'DKL_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE02), '1900-01-01')) is null and 
                    src:DKL_UDFDATE02 is not null) THEN
                    'DKL_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE03), '1900-01-01')) is null and 
                    src:DKL_UDFDATE03 is not null) THEN
                    'DKL_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE04), '1900-01-01')) is null and 
                    src:DKL_UDFDATE04 is not null) THEN
                    'DKL_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE05), '1900-01-01')) is null and 
                    src:DKL_UDFDATE05 is not null) THEN
                    'DKL_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM01), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM01 is not null) THEN
                    'DKL_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM02), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM02 is not null) THEN
                    'DKL_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM03), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM03 is not null) THEN
                    'DKL_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM04), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM04 is not null) THEN
                    'DKL_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM05), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM05 is not null) THEN
                    'DKL_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DKL_UPDATECOUNT is not null) THEN
                    'DKL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DKL_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UPDATED), '1900-01-01')) is null and 
                    src:DKL_UPDATED is not null) THEN
                    'DKL_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is null and 
                src:DKL_LASTSAVED is not null) THEN
                'DKL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DKL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DKL_DCKCODE,
                src:DKL_LINE  ORDER BY 
            src:DKL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCKLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_ACD), '0'), 38, 10) is null and 
                    src:DKL_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_COUNTQTY), '0'), 38, 10) is null and 
                    src:DKL_COUNTQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_INSPDATE), '1900-01-01')) is null and 
                    src:DKL_INSPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_INSPECTEDQTY), '0'), 38, 10) is null and 
                    src:DKL_INSPECTEDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_INSPREJQTY), '0'), 38, 10) is null and 
                    src:DKL_INSPREJQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is null and 
                    src:DKL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_LINE), '0'), 38, 10) is null and 
                    src:DKL_LINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_MANLOTEXP), '1900-01-01')) is null and 
                    src:DKL_MANLOTEXP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_MOBILEDATEUPDATED), '1900-01-01')) is null and 
                    src:DKL_MOBILEDATEUPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_ORDLINE), '0'), 38, 10) is null and 
                    src:DKL_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_PRINT), '0'), 38, 10) is null and 
                    src:DKL_PRINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_RECVQTY), '0'), 38, 10) is null and 
                    src:DKL_RECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_REPAIRPRICE), '0'), 38, 10) is null and 
                    src:DKL_REPAIRPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_RETNQTY), '0'), 38, 10) is null and 
                    src:DKL_RETNQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:DKL_SCRAPQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE01), '1900-01-01')) is null and 
                    src:DKL_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE02), '1900-01-01')) is null and 
                    src:DKL_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE03), '1900-01-01')) is null and 
                    src:DKL_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE04), '1900-01-01')) is null and 
                    src:DKL_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UDFDATE05), '1900-01-01')) is null and 
                    src:DKL_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM01), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM02), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM03), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM04), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UDFNUM05), '0'), 38, 10) is null and 
                    src:DKL_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DKL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DKL_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_UPDATED), '1900-01-01')) is null and 
                    src:DKL_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is null and 
                src:DKL_LASTSAVED is not null)