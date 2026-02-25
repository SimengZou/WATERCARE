CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5STOCK_ERROR AS SELECT src, 'EAM_R5STOCK' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_AVGPRICE), '0'), 38, 10) is null and 
                    src:STO_AVGPRICE is not null) THEN
                    'STO_AVGPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_AVGPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_BASEPRICE), '0'), 38, 10) is null and 
                    src:STO_BASEPRICE is not null) THEN
                    'STO_BASEPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_BASEPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_COREVALUE), '0'), 38, 10) is null and 
                    src:STO_COREVALUE is not null) THEN
                    'STO_COREVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_COREVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_CREATED), '1900-01-01')) is null and 
                    src:STO_CREATED is not null) THEN
                    'STO_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_CREDIT), '0'), 38, 10) is null and 
                    src:STO_CREDIT is not null) THEN
                    'STO_CREDIT contains a non-numeric value : \'' || AS_VARCHAR(src:STO_CREDIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_FORINV), '0'), 38, 10) is null and 
                    src:STO_FORINV is not null) THEN
                    'STO_FORINV contains a non-numeric value : \'' || AS_VARCHAR(src:STO_FORINV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_LASTPRICE), '0'), 38, 10) is null and 
                    src:STO_LASTPRICE is not null) THEN
                    'STO_LASTPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_LASTPRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_LASTSAVED), '1900-01-01')) is null and 
                    src:STO_LASTSAVED is not null) THEN
                    'STO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_LEADTIME), '0'), 38, 10) is null and 
                    src:STO_LEADTIME is not null) THEN
                    'STO_LEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:STO_LEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_MAXQTY), '0'), 38, 10) is null and 
                    src:STO_MAXQTY is not null) THEN
                    'STO_MAXQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_MAXQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_MINLEV), '0'), 38, 10) is null and 
                    src:STO_MINLEV is not null) THEN
                    'STO_MINLEV contains a non-numeric value : \'' || AS_VARCHAR(src:STO_MINLEV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_ORDLEV), '0'), 38, 10) is null and 
                    src:STO_ORDLEV is not null) THEN
                    'STO_ORDLEV contains a non-numeric value : \'' || AS_VARCHAR(src:STO_ORDLEV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_ORDQTY), '0'), 38, 10) is null and 
                    src:STO_ORDQTY is not null) THEN
                    'STO_ORDQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_ORDQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_PREFPRICE), '0'), 38, 10) is null and 
                    src:STO_PREFPRICE is not null) THEN
                    'STO_PREFPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_PREFPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_QTY), '0'), 38, 10) is null and 
                    src:STO_QTY is not null) THEN
                    'STO_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPAIRQTY), '0'), 38, 10) is null and 
                    src:STO_REPAIRQTY is not null) THEN
                    'STO_REPAIRQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_REPAIRQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPLEADTIME), '0'), 38, 10) is null and 
                    src:STO_REPLEADTIME is not null) THEN
                    'STO_REPLEADTIME contains a non-numeric value : \'' || AS_VARCHAR(src:STO_REPLEADTIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPMINQTY), '0'), 38, 10) is null and 
                    src:STO_REPMINQTY is not null) THEN
                    'STO_REPMINQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_REPMINQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPPRICE), '0'), 38, 10) is null and 
                    src:STO_REPPRICE is not null) THEN
                    'STO_REPPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_REPPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_SHOPQTY), '0'), 38, 10) is null and 
                    src:STO_SHOPQTY is not null) THEN
                    'STO_SHOPQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_SHOPQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_STDPRICE), '0'), 38, 10) is null and 
                    src:STO_STDPRICE is not null) THEN
                    'STO_STDPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:STO_STDPRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_STOCKTAKE), '1900-01-01')) is null and 
                    src:STO_STOCKTAKE is not null) THEN
                    'STO_STOCKTAKE contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_STOCKTAKE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE01), '1900-01-01')) is null and 
                    src:STO_UDFDATE01 is not null) THEN
                    'STO_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE02), '1900-01-01')) is null and 
                    src:STO_UDFDATE02 is not null) THEN
                    'STO_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE03), '1900-01-01')) is null and 
                    src:STO_UDFDATE03 is not null) THEN
                    'STO_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE04), '1900-01-01')) is null and 
                    src:STO_UDFDATE04 is not null) THEN
                    'STO_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE05), '1900-01-01')) is null and 
                    src:STO_UDFDATE05 is not null) THEN
                    'STO_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM01), '0'), 38, 10) is null and 
                    src:STO_UDFNUM01 is not null) THEN
                    'STO_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM02), '0'), 38, 10) is null and 
                    src:STO_UDFNUM02 is not null) THEN
                    'STO_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM03), '0'), 38, 10) is null and 
                    src:STO_UDFNUM03 is not null) THEN
                    'STO_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM04), '0'), 38, 10) is null and 
                    src:STO_UDFNUM04 is not null) THEN
                    'STO_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM05), '0'), 38, 10) is null and 
                    src:STO_UDFNUM05 is not null) THEN
                    'STO_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STO_UPDATECOUNT is not null) THEN
                    'STO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:STO_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UPDATED), '1900-01-01')) is null and 
                    src:STO_UPDATED is not null) THEN
                    'STO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_VENDORQTY), '0'), 38, 10) is null and 
                    src:STO_VENDORQTY is not null) THEN
                    'STO_VENDORQTY contains a non-numeric value : \'' || AS_VARCHAR(src:STO_VENDORQTY) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_LASTSAVED), '1900-01-01')) is null and 
                src:STO_LASTSAVED is not null) THEN
                'STO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:STO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:STO_PART,
                src:STO_PART_ORG,
                src:STO_STORE  ORDER BY 
            src:STO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STOCK as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_AVGPRICE), '0'), 38, 10) is null and 
                    src:STO_AVGPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_BASEPRICE), '0'), 38, 10) is null and 
                    src:STO_BASEPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_COREVALUE), '0'), 38, 10) is null and 
                    src:STO_COREVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_CREATED), '1900-01-01')) is null and 
                    src:STO_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_CREDIT), '0'), 38, 10) is null and 
                    src:STO_CREDIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_FORINV), '0'), 38, 10) is null and 
                    src:STO_FORINV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_LASTPRICE), '0'), 38, 10) is null and 
                    src:STO_LASTPRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_LASTSAVED), '1900-01-01')) is null and 
                    src:STO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_LEADTIME), '0'), 38, 10) is null and 
                    src:STO_LEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_MAXQTY), '0'), 38, 10) is null and 
                    src:STO_MAXQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_MINLEV), '0'), 38, 10) is null and 
                    src:STO_MINLEV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_ORDLEV), '0'), 38, 10) is null and 
                    src:STO_ORDLEV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_ORDQTY), '0'), 38, 10) is null and 
                    src:STO_ORDQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_PREFPRICE), '0'), 38, 10) is null and 
                    src:STO_PREFPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_QTY), '0'), 38, 10) is null and 
                    src:STO_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPAIRQTY), '0'), 38, 10) is null and 
                    src:STO_REPAIRQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPLEADTIME), '0'), 38, 10) is null and 
                    src:STO_REPLEADTIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPMINQTY), '0'), 38, 10) is null and 
                    src:STO_REPMINQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_REPPRICE), '0'), 38, 10) is null and 
                    src:STO_REPPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_SHOPQTY), '0'), 38, 10) is null and 
                    src:STO_SHOPQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_STDPRICE), '0'), 38, 10) is null and 
                    src:STO_STDPRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_STOCKTAKE), '1900-01-01')) is null and 
                    src:STO_STOCKTAKE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE01), '1900-01-01')) is null and 
                    src:STO_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE02), '1900-01-01')) is null and 
                    src:STO_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE03), '1900-01-01')) is null and 
                    src:STO_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE04), '1900-01-01')) is null and 
                    src:STO_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UDFDATE05), '1900-01-01')) is null and 
                    src:STO_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM01), '0'), 38, 10) is null and 
                    src:STO_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM02), '0'), 38, 10) is null and 
                    src:STO_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM03), '0'), 38, 10) is null and 
                    src:STO_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM04), '0'), 38, 10) is null and 
                    src:STO_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UDFNUM05), '0'), 38, 10) is null and 
                    src:STO_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:STO_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_UPDATED), '1900-01-01')) is null and 
                    src:STO_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STO_VENDORQTY), '0'), 38, 10) is null and 
                    src:STO_VENDORQTY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STO_LASTSAVED), '1900-01-01')) is null and 
                src:STO_LASTSAVED is not null)