CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ACTIVITIES_ERROR AS SELECT src, 'EAM_R5ACTIVITIES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is null and 
                    src:ACT_ACT is not null) THEN
                    'ACT_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DEFERREDORIGACT), '0'), 38, 10) is null and 
                    src:ACT_DEFERREDORIGACT is not null) THEN
                    'ACT_DEFERREDORIGACT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_DEFERREDORIGACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is null and 
                    src:ACT_DURATION is not null) THEN
                    'ACT_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is null and 
                    src:ACT_EST is not null) THEN
                    'ACT_EST contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_EST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTLABORCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTLABORCOST is not null) THEN
                    'ACT_ESTLABORCOST contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ESTLABORCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTMATLCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTMATLCOST is not null) THEN
                    'ACT_ESTMATLCOST contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ESTMATLCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTMISCCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTMISCCOST is not null) THEN
                    'ACT_ESTMISCCOST contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ESTMISCCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROMPOINT), '0'), 38, 10) is null and 
                    src:ACT_FROMPOINT is not null) THEN
                    'ACT_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_HORIZONTAL_OFFSET is not null) THEN
                    'ACT_FROM_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:ACT_FROM_LATITUDE is not null) THEN
                    'ACT_FROM_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACT_FROM_LONGITUDE is not null) THEN
                    'ACT_FROM_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_OFFSET is not null) THEN
                    'ACT_FROM_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACT_FROM_OFFSET_PERCENTAGE is not null) THEN
                    'ACT_FROM_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ACT_FROM_REFERENCE is not null) THEN
                    'ACT_FROM_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_VERTICAL_OFFSET is not null) THEN
                    'ACT_FROM_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_FROM_XCOORDINATE is not null) THEN
                    'ACT_FROM_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_FROM_YCOORDINATE is not null) THEN
                    'ACT_FROM_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_FROM_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_INVOICELINE), '0'), 38, 10) is null and 
                    src:ACT_INVOICELINE is not null) THEN
                    'ACT_INVOICELINE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_INVOICELINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:ACT_INVOICE_REVISION is not null) THEN
                    'ACT_INVOICE_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_INVOICE_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                    src:ACT_LASTSAVED is not null) THEN
                    'ACT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LATESTSCHED), '1900-01-01')) is null and 
                    src:ACT_LATESTSCHED is not null) THEN
                    'ACT_LATESTSCHED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_LATESTSCHED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_LEVEL), '0'), 38, 10) is null and 
                    src:ACT_LEVEL is not null) THEN
                    'ACT_LEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_LEVEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_MATLREV), '0'), 38, 10) is null and 
                    src:ACT_MATLREV is not null) THEN
                    'ACT_MATLREV contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_MATLREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_MAXDUR), '0'), 38, 10) is null and 
                    src:ACT_MAXDUR is not null) THEN
                    'ACT_MAXDUR contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_MAXDUR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NEWDUR), '0'), 38, 10) is null and 
                    src:ACT_NEWDUR is not null) THEN
                    'ACT_NEWDUR contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_NEWDUR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_NEWSTART), '1900-01-01')) is null and 
                    src:ACT_NEWSTART is not null) THEN
                    'ACT_NEWSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_NEWSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NT), '0'), 38, 10) is null and 
                    src:ACT_NT is not null) THEN
                    'ACT_NT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_NT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NTRATE), '0'), 38, 10) is null and 
                    src:ACT_NTRATE is not null) THEN
                    'ACT_NTRATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_NTRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ORDLINE), '0'), 38, 10) is null and 
                    src:ACT_ORDLINE is not null) THEN
                    'ACT_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_OT), '0'), 38, 10) is null and 
                    src:ACT_OT is not null) THEN
                    'ACT_OT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_OT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_OTRATE), '0'), 38, 10) is null and 
                    src:ACT_OTRATE is not null) THEN
                    'ACT_OTRATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_OTRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PARENT), '0'), 38, 10) is null and 
                    src:ACT_PARENT is not null) THEN
                    'ACT_PARENT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_PARENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERCOMPLETE), '0'), 38, 10) is null and 
                    src:ACT_PERCOMPLETE is not null) THEN
                    'ACT_PERCOMPLETE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_PERCOMPLETE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_PERFORMED), '1900-01-01')) is null and 
                    src:ACT_PERFORMED is not null) THEN
                    'ACT_PERFORMED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_PERFORMED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_PERFORMED2), '1900-01-01')) is null and 
                    src:ACT_PERFORMED2 is not null) THEN
                    'ACT_PERFORMED2 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_PERFORMED2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is null and 
                    src:ACT_PERSONS is not null) THEN
                    'ACT_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PURRATE), '0'), 38, 10) is null and 
                    src:ACT_PURRATE is not null) THEN
                    'ACT_PURRATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_PURRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_QTY), '0'), 38, 10) is null and 
                    src:ACT_QTY is not null) THEN
                    'ACT_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is null and 
                    src:ACT_REM is not null) THEN
                    'ACT_REM contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_REM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REQLINE), '0'), 38, 10) is null and 
                    src:ACT_REQLINE is not null) THEN
                    'ACT_REQLINE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_REQLINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_REVIEWED), '1900-01-01')) is null and 
                    src:ACT_REVIEWED is not null) THEN
                    'ACT_REVIEWED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_REVIEWED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SCHEDHRS), '0'), 38, 10) is null and 
                    src:ACT_SCHEDHRS is not null) THEN
                    'ACT_SCHEDHRS contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_SCHEDHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SCHEDULINGSESSIONID), '0'), 38, 10) is null and 
                    src:ACT_SCHEDULINGSESSIONID is not null) THEN
                    'ACT_SCHEDULINGSESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_SCHEDULINGSESSIONID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SEQ), '0'), 38, 10) is null and 
                    src:ACT_SEQ is not null) THEN
                    'ACT_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_SEQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_START), '1900-01-01')) is null and 
                    src:ACT_START is not null) THEN
                    'ACT_START contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TASKREV), '0'), 38, 10) is null and 
                    src:ACT_TASKREV is not null) THEN
                    'ACT_TASKREV contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TASKREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TIME), '0'), 38, 10) is null and 
                    src:ACT_TIME is not null) THEN
                    'ACT_TIME contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TOPOINT), '0'), 38, 10) is null and 
                    src:ACT_TOPOINT is not null) THEN
                    'ACT_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TOPPARENT), '0'), 38, 10) is null and 
                    src:ACT_TOPPARENT is not null) THEN
                    'ACT_TOPPARENT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TOPPARENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_HORIZONTAL_OFFSET is not null) THEN
                    'ACT_TO_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:ACT_TO_LATITUDE is not null) THEN
                    'ACT_TO_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACT_TO_LONGITUDE is not null) THEN
                    'ACT_TO_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_OFFSET is not null) THEN
                    'ACT_TO_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACT_TO_OFFSET_PERCENTAGE is not null) THEN
                    'ACT_TO_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ACT_TO_REFERENCE is not null) THEN
                    'ACT_TO_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_VERTICAL_OFFSET is not null) THEN
                    'ACT_TO_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_TO_XCOORDINATE is not null) THEN
                    'ACT_TO_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_TO_YCOORDINATE is not null) THEN
                    'ACT_TO_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_TO_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE01), '1900-01-01')) is null and 
                    src:ACT_UDFDATE01 is not null) THEN
                    'ACT_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE02), '1900-01-01')) is null and 
                    src:ACT_UDFDATE02 is not null) THEN
                    'ACT_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE03), '1900-01-01')) is null and 
                    src:ACT_UDFDATE03 is not null) THEN
                    'ACT_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE04), '1900-01-01')) is null and 
                    src:ACT_UDFDATE04 is not null) THEN
                    'ACT_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE05), '1900-01-01')) is null and 
                    src:ACT_UDFDATE05 is not null) THEN
                    'ACT_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM01), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM01 is not null) THEN
                    'ACT_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM02), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM02 is not null) THEN
                    'ACT_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM03), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM03 is not null) THEN
                    'ACT_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM04), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM04 is not null) THEN
                    'ACT_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM05), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM05 is not null) THEN
                    'ACT_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACT_UPDATECOUNT is not null) THEN
                    'ACT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ACT_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                src:ACT_LASTSAVED is not null) THEN
                'ACT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ACT_ACT,
                src:ACT_EVENT  ORDER BY 
            src:ACT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTIVITIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is null and 
                    src:ACT_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DEFERREDORIGACT), '0'), 38, 10) is null and 
                    src:ACT_DEFERREDORIGACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is null and 
                    src:ACT_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is null and 
                    src:ACT_EST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTLABORCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTLABORCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTMATLCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTMATLCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ESTMISCCOST), '0'), 38, 10) is null and 
                    src:ACT_ESTMISCCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROMPOINT), '0'), 38, 10) is null and 
                    src:ACT_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:ACT_FROM_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACT_FROM_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACT_FROM_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ACT_FROM_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_FROM_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_FROM_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_FROM_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_INVOICELINE), '0'), 38, 10) is null and 
                    src:ACT_INVOICELINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:ACT_INVOICE_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                    src:ACT_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LATESTSCHED), '1900-01-01')) is null and 
                    src:ACT_LATESTSCHED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_LEVEL), '0'), 38, 10) is null and 
                    src:ACT_LEVEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_MATLREV), '0'), 38, 10) is null and 
                    src:ACT_MATLREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_MAXDUR), '0'), 38, 10) is null and 
                    src:ACT_MAXDUR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NEWDUR), '0'), 38, 10) is null and 
                    src:ACT_NEWDUR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_NEWSTART), '1900-01-01')) is null and 
                    src:ACT_NEWSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NT), '0'), 38, 10) is null and 
                    src:ACT_NT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_NTRATE), '0'), 38, 10) is null and 
                    src:ACT_NTRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_ORDLINE), '0'), 38, 10) is null and 
                    src:ACT_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_OT), '0'), 38, 10) is null and 
                    src:ACT_OT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_OTRATE), '0'), 38, 10) is null and 
                    src:ACT_OTRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PARENT), '0'), 38, 10) is null and 
                    src:ACT_PARENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERCOMPLETE), '0'), 38, 10) is null and 
                    src:ACT_PERCOMPLETE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_PERFORMED), '1900-01-01')) is null and 
                    src:ACT_PERFORMED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_PERFORMED2), '1900-01-01')) is null and 
                    src:ACT_PERFORMED2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is null and 
                    src:ACT_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_PURRATE), '0'), 38, 10) is null and 
                    src:ACT_PURRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_QTY), '0'), 38, 10) is null and 
                    src:ACT_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is null and 
                    src:ACT_REM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_REQLINE), '0'), 38, 10) is null and 
                    src:ACT_REQLINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_REVIEWED), '1900-01-01')) is null and 
                    src:ACT_REVIEWED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SCHEDHRS), '0'), 38, 10) is null and 
                    src:ACT_SCHEDHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SCHEDULINGSESSIONID), '0'), 38, 10) is null and 
                    src:ACT_SCHEDULINGSESSIONID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_SEQ), '0'), 38, 10) is null and 
                    src:ACT_SEQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_START), '1900-01-01')) is null and 
                    src:ACT_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TASKREV), '0'), 38, 10) is null and 
                    src:ACT_TASKREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TIME), '0'), 38, 10) is null and 
                    src:ACT_TIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TOPOINT), '0'), 38, 10) is null and 
                    src:ACT_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TOPPARENT), '0'), 38, 10) is null and 
                    src:ACT_TOPPARENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:ACT_TO_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACT_TO_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACT_TO_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ACT_TO_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACT_TO_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_TO_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACT_TO_YCOORDINATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE01), '1900-01-01')) is null and 
                    src:ACT_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE02), '1900-01-01')) is null and 
                    src:ACT_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE03), '1900-01-01')) is null and 
                    src:ACT_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE04), '1900-01-01')) is null and 
                    src:ACT_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_UDFDATE05), '1900-01-01')) is null and 
                    src:ACT_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM01), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM02), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM03), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM04), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UDFNUM05), '0'), 38, 10) is null and 
                    src:ACT_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACT_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is null and 
                src:ACT_LASTSAVED is not null)