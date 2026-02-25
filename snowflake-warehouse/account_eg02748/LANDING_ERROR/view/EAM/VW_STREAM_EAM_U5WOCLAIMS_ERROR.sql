CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_U5WOCLAIMS_ERROR AS SELECT src, 'EAM_U5WOCLAIMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATED), '1900-01-01')) is null and 
                    src:CREATED is not null) THEN
                    'CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                    src:LASTSAVED is not null) THEN
                    'LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UPDATECOUNT is not null) THEN
                    'UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UPDATED), '1900-01-01')) is null and 
                    src:UPDATED is not null) THEN
                    'UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCO_CHARGEDATE), '1900-01-01')) is null and 
                    src:WCO_CHARGEDATE is not null) THEN
                    'WCO_CHARGEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WCO_CHARGEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_CONTRACTOR_QTY), '0'), 38, 10) is null and 
                    src:WCO_CONTRACTOR_QTY is not null) THEN
                    'WCO_CONTRACTOR_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:WCO_CONTRACTOR_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_CONTRACTOR_RATE), '0'), 38, 10) is null and 
                    src:WCO_CONTRACTOR_RATE is not null) THEN
                    'WCO_CONTRACTOR_RATE contains a non-numeric value : \'' || AS_VARCHAR(src:WCO_CONTRACTOR_RATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_LINETOTAL), '0'), 38, 10) is null and 
                    src:WCO_LINETOTAL is not null) THEN
                    'WCO_LINETOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:WCO_LINETOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_SCHEDITEM_RATE), '0'), 38, 10) is null and 
                    src:WCO_SCHEDITEM_RATE is not null) THEN
                    'WCO_SCHEDITEM_RATE contains a non-numeric value : \'' || AS_VARCHAR(src:WCO_SCHEDITEM_RATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                src:LASTSAVED is not null) THEN
                'LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WCO_EVENT,
                src:WCO_ORG,
                src:WCO_PK  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5WOCLAIMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATED), '1900-01-01')) is null and 
                    src:CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                    src:LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UPDATED), '1900-01-01')) is null and 
                    src:UPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WCO_CHARGEDATE), '1900-01-01')) is null and 
                    src:WCO_CHARGEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_CONTRACTOR_QTY), '0'), 38, 10) is null and 
                    src:WCO_CONTRACTOR_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_CONTRACTOR_RATE), '0'), 38, 10) is null and 
                    src:WCO_CONTRACTOR_RATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_LINETOTAL), '0'), 38, 10) is null and 
                    src:WCO_LINETOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WCO_SCHEDITEM_RATE), '0'), 38, 10) is null and 
                    src:WCO_SCHEDITEM_RATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is null and 
                src:LASTSAVED is not null)