CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORDERLINES_ERROR AS SELECT src, 'EAM_R5ORDERLINES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ACD), '0'), 38, 10) is null and 
                    src:ORL_ACD is not null) THEN
                    'ORL_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ACT), '0'), 38, 10) is null and 
                    src:ORL_ACT is not null) THEN
                    'ORL_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_BLANKETLINE), '0'), 38, 10) is null and 
                    src:ORL_BLANKETLINE is not null) THEN
                    'ORL_BLANKETLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_BLANKETLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_DCKRECVQTY), '0'), 38, 10) is null and 
                    src:ORL_DCKRECVQTY is not null) THEN
                    'ORL_DCKRECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_DCKRECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_DISCPERC), '0'), 38, 10) is null and 
                    src:ORL_DISCPERC is not null) THEN
                    'ORL_DISCPERC contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_DISCPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_DUE), '1900-01-01')) is null and 
                    src:ORL_DUE is not null) THEN
                    'ORL_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCH), '0'), 38, 10) is null and 
                    src:ORL_EXCH is not null) THEN
                    'ORL_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_EXCH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORL_EXCHFROMDUAL is not null) THEN
                    'ORL_EXCHFROMDUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_EXCHFROMDUAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORL_EXCHTODUAL is not null) THEN
                    'ORL_EXCHTODUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_EXCHTODUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_GLTRANSFER), '1900-01-01')) is null and 
                    src:ORL_GLTRANSFER is not null) THEN
                    'ORL_GLTRANSFER contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_GLTRANSFER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_INTERFACE), '1900-01-01')) is null and 
                    src:ORL_INTERFACE is not null) THEN
                    'ORL_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVCALCTAXAMOUNT), '0'), 38, 10) is null and 
                    src:ORL_INVCALCTAXAMOUNT is not null) THEN
                    'ORL_INVCALCTAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_INVCALCTAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVQTY), '0'), 38, 10) is null and 
                    src:ORL_INVQTY is not null) THEN
                    'ORL_INVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_INVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVVALUE), '0'), 38, 10) is null and 
                    src:ORL_INVVALUE is not null) THEN
                    'ORL_INVVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_INVVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_IPERROR), '0'), 38, 10) is null and 
                    src:ORL_IPERROR is not null) THEN
                    'ORL_IPERROR contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_IPERROR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_IPLASTUPDATE), '1900-01-01')) is null and 
                    src:ORL_IPLASTUPDATE is not null) THEN
                    'ORL_IPLASTUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_IPLASTUPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_LASTSAVED), '1900-01-01')) is null and 
                    src:ORL_LASTSAVED is not null) THEN
                    'ORL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_MULTIPLY), '0'), 38, 10) is null and 
                    src:ORL_MULTIPLY is not null) THEN
                    'ORL_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ORDLINE), '0'), 38, 10) is null and 
                    src:ORL_ORDLINE is not null) THEN
                    'ORL_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ORDQTY), '0'), 38, 10) is null and 
                    src:ORL_ORDQTY is not null) THEN
                    'ORL_ORDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_ORDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_PAYMENTNUMBER), '0'), 38, 10) is null and 
                    src:ORL_PAYMENTNUMBER is not null) THEN
                    'ORL_PAYMENTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_PAYMENTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_PRICE), '0'), 38, 10) is null and 
                    src:ORL_PRICE is not null) THEN
                    'ORL_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_PRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_QUOTLINE), '0'), 38, 10) is null and 
                    src:ORL_QUOTLINE is not null) THEN
                    'ORL_QUOTLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_QUOTLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVQTY), '0'), 38, 10) is null and 
                    src:ORL_RECVQTY is not null) THEN
                    'ORL_RECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_RECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVTASKQTY), '0'), 38, 10) is null and 
                    src:ORL_RECVTASKQTY is not null) THEN
                    'ORL_RECVTASKQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_RECVTASKQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVVALUE), '0'), 38, 10) is null and 
                    src:ORL_RECVVALUE is not null) THEN
                    'ORL_RECVVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_RECVVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_REQLINE), '0'), 38, 10) is null and 
                    src:ORL_REQLINE is not null) THEN
                    'ORL_REQLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_REQLINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_REVISED), '1900-01-01')) is null and 
                    src:ORL_REVISED is not null) THEN
                    'ORL_REVISED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_REVISED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_REVISION), '0'), 38, 10) is null and 
                    src:ORL_REVISION is not null) THEN
                    'ORL_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:ORL_SCRAPQTY is not null) THEN
                    'ORL_SCRAPQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_SCRAPQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TASKQTY), '0'), 38, 10) is null and 
                    src:ORL_TASKQTY is not null) THEN
                    'ORL_TASKQTY contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_TASKQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TASKREV), '0'), 38, 10) is null and 
                    src:ORL_TASKREV is not null) THEN
                    'ORL_TASKREV contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_TASKREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TOTEXTRA), '0'), 38, 10) is null and 
                    src:ORL_TOTEXTRA is not null) THEN
                    'ORL_TOTEXTRA contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_TOTEXTRA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TOTTAXAMOUNT), '0'), 38, 10) is null and 
                    src:ORL_TOTTAXAMOUNT is not null) THEN
                    'ORL_TOTTAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_TOTTAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE01), '1900-01-01')) is null and 
                    src:ORL_UDFDATE01 is not null) THEN
                    'ORL_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE02), '1900-01-01')) is null and 
                    src:ORL_UDFDATE02 is not null) THEN
                    'ORL_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE03), '1900-01-01')) is null and 
                    src:ORL_UDFDATE03 is not null) THEN
                    'ORL_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE04), '1900-01-01')) is null and 
                    src:ORL_UDFDATE04 is not null) THEN
                    'ORL_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE05), '1900-01-01')) is null and 
                    src:ORL_UDFDATE05 is not null) THEN
                    'ORL_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM01 is not null) THEN
                    'ORL_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM02 is not null) THEN
                    'ORL_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM03 is not null) THEN
                    'ORL_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM04 is not null) THEN
                    'ORL_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM05 is not null) THEN
                    'ORL_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORL_UPDATECOUNT is not null) THEN
                    'ORL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_LASTSAVED), '1900-01-01')) is null and 
                src:ORL_LASTSAVED is not null) THEN
                'ORL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ORL_ORDER,
                src:ORL_ORDER_ORG,
                src:ORL_ORDLINE  ORDER BY 
            src:ORL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORDERLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ACD), '0'), 38, 10) is null and 
                    src:ORL_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ACT), '0'), 38, 10) is null and 
                    src:ORL_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_BLANKETLINE), '0'), 38, 10) is null and 
                    src:ORL_BLANKETLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_DCKRECVQTY), '0'), 38, 10) is null and 
                    src:ORL_DCKRECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_DISCPERC), '0'), 38, 10) is null and 
                    src:ORL_DISCPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_DUE), '1900-01-01')) is null and 
                    src:ORL_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCH), '0'), 38, 10) is null and 
                    src:ORL_EXCH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORL_EXCHFROMDUAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORL_EXCHTODUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_GLTRANSFER), '1900-01-01')) is null and 
                    src:ORL_GLTRANSFER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_INTERFACE), '1900-01-01')) is null and 
                    src:ORL_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVCALCTAXAMOUNT), '0'), 38, 10) is null and 
                    src:ORL_INVCALCTAXAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVQTY), '0'), 38, 10) is null and 
                    src:ORL_INVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_INVVALUE), '0'), 38, 10) is null and 
                    src:ORL_INVVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_IPERROR), '0'), 38, 10) is null and 
                    src:ORL_IPERROR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_IPLASTUPDATE), '1900-01-01')) is null and 
                    src:ORL_IPLASTUPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_LASTSAVED), '1900-01-01')) is null and 
                    src:ORL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_MULTIPLY), '0'), 38, 10) is null and 
                    src:ORL_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ORDLINE), '0'), 38, 10) is null and 
                    src:ORL_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_ORDQTY), '0'), 38, 10) is null and 
                    src:ORL_ORDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_PAYMENTNUMBER), '0'), 38, 10) is null and 
                    src:ORL_PAYMENTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_PRICE), '0'), 38, 10) is null and 
                    src:ORL_PRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_QUOTLINE), '0'), 38, 10) is null and 
                    src:ORL_QUOTLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVQTY), '0'), 38, 10) is null and 
                    src:ORL_RECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVTASKQTY), '0'), 38, 10) is null and 
                    src:ORL_RECVTASKQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_RECVVALUE), '0'), 38, 10) is null and 
                    src:ORL_RECVVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_REQLINE), '0'), 38, 10) is null and 
                    src:ORL_REQLINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_REVISED), '1900-01-01')) is null and 
                    src:ORL_REVISED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_REVISION), '0'), 38, 10) is null and 
                    src:ORL_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:ORL_SCRAPQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TASKQTY), '0'), 38, 10) is null and 
                    src:ORL_TASKQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TASKREV), '0'), 38, 10) is null and 
                    src:ORL_TASKREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TOTEXTRA), '0'), 38, 10) is null and 
                    src:ORL_TOTEXTRA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_TOTTAXAMOUNT), '0'), 38, 10) is null and 
                    src:ORL_TOTTAXAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE01), '1900-01-01')) is null and 
                    src:ORL_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE02), '1900-01-01')) is null and 
                    src:ORL_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE03), '1900-01-01')) is null and 
                    src:ORL_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE04), '1900-01-01')) is null and 
                    src:ORL_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_UDFDATE05), '1900-01-01')) is null and 
                    src:ORL_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORL_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORL_LASTSAVED), '1900-01-01')) is null and 
                src:ORL_LASTSAVED is not null)