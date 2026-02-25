CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRANSLINES_ERROR AS SELECT src, 'EAM_R5TRANSLINES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ACD), '0'), 38, 10) is null and 
                    src:TRL_ACD is not null) THEN
                    'TRL_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ACT), '0'), 38, 10) is null and 
                    src:TRL_ACT is not null) THEN
                    'TRL_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_AVGPRICE), '0'), 38, 10) is null and 
                    src:TRL_AVGPRICE is not null) THEN
                    'TRL_AVGPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_AVGPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_CALOTHERCOST), '0'), 38, 10) is null and 
                    src:TRL_CALOTHERCOST is not null) THEN
                    'TRL_CALOTHERCOST contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_CALOTHERCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_CORERETURNPRINTQTY), '0'), 38, 10) is null and 
                    src:TRL_CORERETURNPRINTQTY is not null) THEN
                    'TRL_CORERETURNPRINTQTY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_CORERETURNPRINTQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_CREATED), '1900-01-01')) is null and 
                    src:TRL_CREATED is not null) THEN
                    'TRL_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_DATE), '1900-01-01')) is null and 
                    src:TRL_DATE is not null) THEN
                    'TRL_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_DCKLINE), '0'), 38, 10) is null and 
                    src:TRL_DCKLINE is not null) THEN
                    'TRL_DCKLINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_DCKLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_FLEETMARKUP), '0'), 38, 10) is null and 
                    src:TRL_FLEETMARKUP is not null) THEN
                    'TRL_FLEETMARKUP contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_FLEETMARKUP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_GLTRANSFER), '1900-01-01')) is null and 
                    src:TRL_GLTRANSFER is not null) THEN
                    'TRL_GLTRANSFER contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_GLTRANSFER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_INTERFACE), '1900-01-01')) is null and 
                    src:TRL_INTERFACE is not null) THEN
                    'TRL_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_INVOICELINE), '0'), 38, 10) is null and 
                    src:TRL_INVOICELINE is not null) THEN
                    'TRL_INVOICELINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_INVOICELINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:TRL_INVOICE_REVISION is not null) THEN
                    'TRL_INVOICE_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_INVOICE_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_IO), '0'), 38, 10) is null and 
                    src:TRL_IO is not null) THEN
                    'TRL_IO contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_IO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is null and 
                    src:TRL_LASTSAVED is not null) THEN
                    'TRL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_LINE), '0'), 38, 10) is null and 
                    src:TRL_LINE is not null) THEN
                    'TRL_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_MULTIPLY), '0'), 38, 10) is null and 
                    src:TRL_MULTIPLY is not null) THEN
                    'TRL_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ORDLINE), '0'), 38, 10) is null and 
                    src:TRL_ORDLINE is not null) THEN
                    'TRL_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ORIGQTY), '0'), 38, 10) is null and 
                    src:TRL_ORIGQTY is not null) THEN
                    'TRL_ORIGQTY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_ORIGQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_POEXTRACHARGES), '0'), 38, 10) is null and 
                    src:TRL_POEXTRACHARGES is not null) THEN
                    'TRL_POEXTRACHARGES contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_POEXTRACHARGES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_POTAXAMOUNT), '0'), 38, 10) is null and 
                    src:TRL_POTAXAMOUNT is not null) THEN
                    'TRL_POTAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_POTAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_PRICE), '0'), 38, 10) is null and 
                    src:TRL_PRICE is not null) THEN
                    'TRL_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_PRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_PRINTQTY), '0'), 38, 10) is null and 
                    src:TRL_PRINTQTY is not null) THEN
                    'TRL_PRINTQTY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_PRINTQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_QTY), '0'), 38, 10) is null and 
                    src:TRL_QTY is not null) THEN
                    'TRL_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_REPAIRPRICE), '0'), 38, 10) is null and 
                    src:TRL_REPAIRPRICE is not null) THEN
                    'TRL_REPAIRPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_REPAIRPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_REQLINE), '0'), 38, 10) is null and 
                    src:TRL_REQLINE is not null) THEN
                    'TRL_REQLINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_REQLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:TRL_SCRAPQTY is not null) THEN
                    'TRL_SCRAPQTY contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_SCRAPQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_STOCKPRICE), '0'), 38, 10) is null and 
                    src:TRL_STOCKPRICE is not null) THEN
                    'TRL_STOCKPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_STOCKPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_TRANSGROUP), '0'), 38, 10) is null and 
                    src:TRL_TRANSGROUP is not null) THEN
                    'TRL_TRANSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_TRANSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_TRANSORGID), '0'), 38, 10) is null and 
                    src:TRL_TRANSORGID is not null) THEN
                    'TRL_TRANSORGID contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_TRANSORGID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE01), '1900-01-01')) is null and 
                    src:TRL_UDFDATE01 is not null) THEN
                    'TRL_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE02), '1900-01-01')) is null and 
                    src:TRL_UDFDATE02 is not null) THEN
                    'TRL_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE03), '1900-01-01')) is null and 
                    src:TRL_UDFDATE03 is not null) THEN
                    'TRL_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE04), '1900-01-01')) is null and 
                    src:TRL_UDFDATE04 is not null) THEN
                    'TRL_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE05), '1900-01-01')) is null and 
                    src:TRL_UDFDATE05 is not null) THEN
                    'TRL_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM01 is not null) THEN
                    'TRL_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM02 is not null) THEN
                    'TRL_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM03 is not null) THEN
                    'TRL_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM04 is not null) THEN
                    'TRL_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM05 is not null) THEN
                    'TRL_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRL_UPDATECOUNT is not null) THEN
                    'TRL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TRL_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UPDATED), '1900-01-01')) is null and 
                    src:TRL_UPDATED is not null) THEN
                    'TRL_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is null and 
                src:TRL_LASTSAVED is not null) THEN
                'TRL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TRL_LINE,
                src:TRL_TRANS  ORDER BY 
            src:TRL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRANSLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ACD), '0'), 38, 10) is null and 
                    src:TRL_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ACT), '0'), 38, 10) is null and 
                    src:TRL_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_AVGPRICE), '0'), 38, 10) is null and 
                    src:TRL_AVGPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_CALOTHERCOST), '0'), 38, 10) is null and 
                    src:TRL_CALOTHERCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_CORERETURNPRINTQTY), '0'), 38, 10) is null and 
                    src:TRL_CORERETURNPRINTQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_CREATED), '1900-01-01')) is null and 
                    src:TRL_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_DATE), '1900-01-01')) is null and 
                    src:TRL_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_DCKLINE), '0'), 38, 10) is null and 
                    src:TRL_DCKLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_FLEETMARKUP), '0'), 38, 10) is null and 
                    src:TRL_FLEETMARKUP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_GLTRANSFER), '1900-01-01')) is null and 
                    src:TRL_GLTRANSFER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_INTERFACE), '1900-01-01')) is null and 
                    src:TRL_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_INVOICELINE), '0'), 38, 10) is null and 
                    src:TRL_INVOICELINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:TRL_INVOICE_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_IO), '0'), 38, 10) is null and 
                    src:TRL_IO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is null and 
                    src:TRL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_LINE), '0'), 38, 10) is null and 
                    src:TRL_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_MULTIPLY), '0'), 38, 10) is null and 
                    src:TRL_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ORDLINE), '0'), 38, 10) is null and 
                    src:TRL_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_ORIGQTY), '0'), 38, 10) is null and 
                    src:TRL_ORIGQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_POEXTRACHARGES), '0'), 38, 10) is null and 
                    src:TRL_POEXTRACHARGES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_POTAXAMOUNT), '0'), 38, 10) is null and 
                    src:TRL_POTAXAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_PRICE), '0'), 38, 10) is null and 
                    src:TRL_PRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_PRINTQTY), '0'), 38, 10) is null and 
                    src:TRL_PRINTQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_QTY), '0'), 38, 10) is null and 
                    src:TRL_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_REPAIRPRICE), '0'), 38, 10) is null and 
                    src:TRL_REPAIRPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_REQLINE), '0'), 38, 10) is null and 
                    src:TRL_REQLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_SCRAPQTY), '0'), 38, 10) is null and 
                    src:TRL_SCRAPQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_STOCKPRICE), '0'), 38, 10) is null and 
                    src:TRL_STOCKPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_TRANSGROUP), '0'), 38, 10) is null and 
                    src:TRL_TRANSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_TRANSORGID), '0'), 38, 10) is null and 
                    src:TRL_TRANSORGID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE01), '1900-01-01')) is null and 
                    src:TRL_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE02), '1900-01-01')) is null and 
                    src:TRL_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE03), '1900-01-01')) is null and 
                    src:TRL_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE04), '1900-01-01')) is null and 
                    src:TRL_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UDFDATE05), '1900-01-01')) is null and 
                    src:TRL_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRL_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRL_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_UPDATED), '1900-01-01')) is null and 
                    src:TRL_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is null and 
                src:TRL_LASTSAVED is not null)