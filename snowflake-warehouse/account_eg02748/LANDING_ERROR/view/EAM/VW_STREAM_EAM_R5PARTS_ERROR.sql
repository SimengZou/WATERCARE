CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PARTS_ERROR AS SELECT src, 'EAM_R5PARTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_AVGPRICE), '0'), 38, 10) is null and 
                    src:PAR_AVGPRICE is not null) THEN
                    'PAR_AVGPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_AVGPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_BASEPRICE), '0'), 38, 10) is null and 
                    src:PAR_BASEPRICE is not null) THEN
                    'PAR_BASEPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_BASEPRICE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_COREVALUE), '0'), 38, 10) is null and 
                    src:PAR_COREVALUE is not null) THEN
                    'PAR_COREVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_COREVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_CREATED), '1900-01-01')) is null and 
                    src:PAR_CREATED is not null) THEN
                    'PAR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_INTERFACE), '1900-01-01')) is null and 
                    src:PAR_INTERFACE is not null) THEN
                    'PAR_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_LASTPRICE), '0'), 38, 10) is null and 
                    src:PAR_LASTPRICE is not null) THEN
                    'PAR_LASTPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_LASTPRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSAVED), '1900-01-01')) is null and 
                    src:PAR_LASTSAVED is not null) THEN
                    'PAR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:PAR_LASTSTATUSUPDATE is not null) THEN
                    'PAR_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_PREFSUPPRICE), '0'), 38, 10) is null and 
                    src:PAR_PREFSUPPRICE is not null) THEN
                    'PAR_PREFSUPPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_PREFSUPPRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:PAR_SAFETYREVIEWED is not null) THEN
                    'PAR_SAFETYREVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_SAFETYREVIEWED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PAR_SAFETYREVIEWREQUIRED is not null) THEN
                    'PAR_SAFETYREVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_SAFETYREVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_STDPRICE), '0'), 38, 10) is null and 
                    src:PAR_STDPRICE is not null) THEN
                    'PAR_STDPRICE contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_STDPRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE01), '1900-01-01')) is null and 
                    src:PAR_UDFDATE01 is not null) THEN
                    'PAR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE02), '1900-01-01')) is null and 
                    src:PAR_UDFDATE02 is not null) THEN
                    'PAR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE03), '1900-01-01')) is null and 
                    src:PAR_UDFDATE03 is not null) THEN
                    'PAR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE04), '1900-01-01')) is null and 
                    src:PAR_UDFDATE04 is not null) THEN
                    'PAR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE05), '1900-01-01')) is null and 
                    src:PAR_UDFDATE05 is not null) THEN
                    'PAR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM01), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM01 is not null) THEN
                    'PAR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM02), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM02 is not null) THEN
                    'PAR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM03), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM03 is not null) THEN
                    'PAR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM04), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM04 is not null) THEN
                    'PAR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM05), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM05 is not null) THEN
                    'PAR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PAR_UPDATECOUNT is not null) THEN
                    'PAR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UPDATED), '1900-01-01')) is null and 
                    src:PAR_UPDATED is not null) THEN
                    'PAR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_WARDAYS), '0'), 38, 10) is null and 
                    src:PAR_WARDAYS is not null) THEN
                    'PAR_WARDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:PAR_WARDAYS) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSAVED), '1900-01-01')) is null and 
                src:PAR_LASTSAVED is not null) THEN
                'PAR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PAR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PAR_CODE,
                src:PAR_ORG  ORDER BY 
            src:PAR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PARTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_AVGPRICE), '0'), 38, 10) is null and 
                    src:PAR_AVGPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_BASEPRICE), '0'), 38, 10) is null and 
                    src:PAR_BASEPRICE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_COREVALUE), '0'), 38, 10) is null and 
                    src:PAR_COREVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_CREATED), '1900-01-01')) is null and 
                    src:PAR_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_INTERFACE), '1900-01-01')) is null and 
                    src:PAR_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_LASTPRICE), '0'), 38, 10) is null and 
                    src:PAR_LASTPRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSAVED), '1900-01-01')) is null and 
                    src:PAR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:PAR_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_PREFSUPPRICE), '0'), 38, 10) is null and 
                    src:PAR_PREFSUPPRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_SAFETYREVIEWED), '1900-01-01')) is null and 
                    src:PAR_SAFETYREVIEWED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_SAFETYREVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PAR_SAFETYREVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_STDPRICE), '0'), 38, 10) is null and 
                    src:PAR_STDPRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE01), '1900-01-01')) is null and 
                    src:PAR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE02), '1900-01-01')) is null and 
                    src:PAR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE03), '1900-01-01')) is null and 
                    src:PAR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE04), '1900-01-01')) is null and 
                    src:PAR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UDFDATE05), '1900-01-01')) is null and 
                    src:PAR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM01), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM02), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM03), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM04), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UDFNUM05), '0'), 38, 10) is null and 
                    src:PAR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PAR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_UPDATED), '1900-01-01')) is null and 
                    src:PAR_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAR_WARDAYS), '0'), 38, 10) is null and 
                    src:PAR_WARDAYS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAR_LASTSAVED), '1900-01-01')) is null and 
                src:PAR_LASTSAVED is not null)