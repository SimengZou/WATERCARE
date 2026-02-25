CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORLREVISIONS_ERROR AS SELECT src, 'EAM_R5ORLREVISIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ACD), '0'), 38, 10) is null and 
                    src:OLR_ACD is not null) THEN
                    'OLR_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ACT), '0'), 38, 10) is null and 
                    src:OLR_ACT is not null) THEN
                    'OLR_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_BLANKETLINE), '0'), 38, 10) is null and 
                    src:OLR_BLANKETLINE is not null) THEN
                    'OLR_BLANKETLINE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_BLANKETLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_DCKRECVQTY), '0'), 38, 10) is null and 
                    src:OLR_DCKRECVQTY is not null) THEN
                    'OLR_DCKRECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_DCKRECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_DISCPERC), '0'), 38, 10) is null and 
                    src:OLR_DISCPERC is not null) THEN
                    'OLR_DISCPERC contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_DISCPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_DUE), '1900-01-01')) is null and 
                    src:OLR_DUE is not null) THEN
                    'OLR_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCH), '0'), 38, 10) is null and 
                    src:OLR_EXCH is not null) THEN
                    'OLR_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_EXCH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:OLR_EXCHFROMDUAL is not null) THEN
                    'OLR_EXCHFROMDUAL contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_EXCHFROMDUAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:OLR_EXCHTODUAL is not null) THEN
                    'OLR_EXCHTODUAL contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_EXCHTODUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_INTERFACE), '1900-01-01')) is null and 
                    src:OLR_INTERFACE is not null) THEN
                    'OLR_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_INVQTY), '0'), 38, 10) is null and 
                    src:OLR_INVQTY is not null) THEN
                    'OLR_INVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_INVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_INVVALUE), '0'), 38, 10) is null and 
                    src:OLR_INVVALUE is not null) THEN
                    'OLR_INVVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_INVVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_IPERROR), '0'), 38, 10) is null and 
                    src:OLR_IPERROR is not null) THEN
                    'OLR_IPERROR contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_IPERROR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_LASTSAVED), '1900-01-01')) is null and 
                    src:OLR_LASTSAVED is not null) THEN
                    'OLR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_MULTIPLY), '0'), 38, 10) is null and 
                    src:OLR_MULTIPLY is not null) THEN
                    'OLR_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ORDLINE), '0'), 38, 10) is null and 
                    src:OLR_ORDLINE is not null) THEN
                    'OLR_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ORDQTY), '0'), 38, 10) is null and 
                    src:OLR_ORDQTY is not null) THEN
                    'OLR_ORDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_ORDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_PAYMENTNUMBER), '0'), 38, 10) is null and 
                    src:OLR_PAYMENTNUMBER is not null) THEN
                    'OLR_PAYMENTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_PAYMENTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_PRICE), '0'), 38, 10) is null and 
                    src:OLR_PRICE is not null) THEN
                    'OLR_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_PRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_QUOTLINE), '0'), 38, 10) is null and 
                    src:OLR_QUOTLINE is not null) THEN
                    'OLR_QUOTLINE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_QUOTLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVQTY), '0'), 38, 10) is null and 
                    src:OLR_RECVQTY is not null) THEN
                    'OLR_RECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_RECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVTASKQTY), '0'), 38, 10) is null and 
                    src:OLR_RECVTASKQTY is not null) THEN
                    'OLR_RECVTASKQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_RECVTASKQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVVALUE), '0'), 38, 10) is null and 
                    src:OLR_RECVVALUE is not null) THEN
                    'OLR_RECVVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_RECVVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_REQLINE), '0'), 38, 10) is null and 
                    src:OLR_REQLINE is not null) THEN
                    'OLR_REQLINE contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_REQLINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_REVISED), '1900-01-01')) is null and 
                    src:OLR_REVISED is not null) THEN
                    'OLR_REVISED contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_REVISED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_REVISION), '0'), 38, 10) is null and 
                    src:OLR_REVISION is not null) THEN
                    'OLR_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_SCRAPQTY), '0'), 38, 10) is null and 
                    src:OLR_SCRAPQTY is not null) THEN
                    'OLR_SCRAPQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_SCRAPQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TASKQTY), '0'), 38, 10) is null and 
                    src:OLR_TASKQTY is not null) THEN
                    'OLR_TASKQTY contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_TASKQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TASKREV), '0'), 38, 10) is null and 
                    src:OLR_TASKREV is not null) THEN
                    'OLR_TASKREV contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_TASKREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TOTEXTRA), '0'), 38, 10) is null and 
                    src:OLR_TOTEXTRA is not null) THEN
                    'OLR_TOTEXTRA contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_TOTEXTRA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TOTTAXAMOUNT), '0'), 38, 10) is null and 
                    src:OLR_TOTTAXAMOUNT is not null) THEN
                    'OLR_TOTTAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_TOTTAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE01), '1900-01-01')) is null and 
                    src:OLR_UDFDATE01 is not null) THEN
                    'OLR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE02), '1900-01-01')) is null and 
                    src:OLR_UDFDATE02 is not null) THEN
                    'OLR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE03), '1900-01-01')) is null and 
                    src:OLR_UDFDATE03 is not null) THEN
                    'OLR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE04), '1900-01-01')) is null and 
                    src:OLR_UDFDATE04 is not null) THEN
                    'OLR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE05), '1900-01-01')) is null and 
                    src:OLR_UDFDATE05 is not null) THEN
                    'OLR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM01), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM01 is not null) THEN
                    'OLR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM02), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM02 is not null) THEN
                    'OLR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM03), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM03 is not null) THEN
                    'OLR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM04), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM04 is not null) THEN
                    'OLR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM05), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM05 is not null) THEN
                    'OLR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OLR_UPDATECOUNT is not null) THEN
                    'OLR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:OLR_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_LASTSAVED), '1900-01-01')) is null and 
                src:OLR_LASTSAVED is not null) THEN
                'OLR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:OLR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:OLR_ORDER,
                src:OLR_ORDER_ORG,
                src:OLR_ORDLINE,
                src:OLR_REVISION  ORDER BY 
            src:OLR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORLREVISIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ACD), '0'), 38, 10) is null and 
                    src:OLR_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ACT), '0'), 38, 10) is null and 
                    src:OLR_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_BLANKETLINE), '0'), 38, 10) is null and 
                    src:OLR_BLANKETLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_DCKRECVQTY), '0'), 38, 10) is null and 
                    src:OLR_DCKRECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_DISCPERC), '0'), 38, 10) is null and 
                    src:OLR_DISCPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_DUE), '1900-01-01')) is null and 
                    src:OLR_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCH), '0'), 38, 10) is null and 
                    src:OLR_EXCH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:OLR_EXCHFROMDUAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:OLR_EXCHTODUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_INTERFACE), '1900-01-01')) is null and 
                    src:OLR_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_INVQTY), '0'), 38, 10) is null and 
                    src:OLR_INVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_INVVALUE), '0'), 38, 10) is null and 
                    src:OLR_INVVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_IPERROR), '0'), 38, 10) is null and 
                    src:OLR_IPERROR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_LASTSAVED), '1900-01-01')) is null and 
                    src:OLR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_MULTIPLY), '0'), 38, 10) is null and 
                    src:OLR_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ORDLINE), '0'), 38, 10) is null and 
                    src:OLR_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_ORDQTY), '0'), 38, 10) is null and 
                    src:OLR_ORDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_PAYMENTNUMBER), '0'), 38, 10) is null and 
                    src:OLR_PAYMENTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_PRICE), '0'), 38, 10) is null and 
                    src:OLR_PRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_QUOTLINE), '0'), 38, 10) is null and 
                    src:OLR_QUOTLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVQTY), '0'), 38, 10) is null and 
                    src:OLR_RECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVTASKQTY), '0'), 38, 10) is null and 
                    src:OLR_RECVTASKQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_RECVVALUE), '0'), 38, 10) is null and 
                    src:OLR_RECVVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_REQLINE), '0'), 38, 10) is null and 
                    src:OLR_REQLINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_REVISED), '1900-01-01')) is null and 
                    src:OLR_REVISED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_REVISION), '0'), 38, 10) is null and 
                    src:OLR_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_SCRAPQTY), '0'), 38, 10) is null and 
                    src:OLR_SCRAPQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TASKQTY), '0'), 38, 10) is null and 
                    src:OLR_TASKQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TASKREV), '0'), 38, 10) is null and 
                    src:OLR_TASKREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TOTEXTRA), '0'), 38, 10) is null and 
                    src:OLR_TOTEXTRA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_TOTTAXAMOUNT), '0'), 38, 10) is null and 
                    src:OLR_TOTTAXAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE01), '1900-01-01')) is null and 
                    src:OLR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE02), '1900-01-01')) is null and 
                    src:OLR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE03), '1900-01-01')) is null and 
                    src:OLR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE04), '1900-01-01')) is null and 
                    src:OLR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_UDFDATE05), '1900-01-01')) is null and 
                    src:OLR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM01), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM02), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM03), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM04), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UDFNUM05), '0'), 38, 10) is null and 
                    src:OLR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:OLR_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OLR_LASTSAVED), '1900-01-01')) is null and 
                src:OLR_LASTSAVED is not null)