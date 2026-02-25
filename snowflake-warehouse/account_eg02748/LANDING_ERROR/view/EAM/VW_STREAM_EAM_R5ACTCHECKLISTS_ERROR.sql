CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ACTCHECKLISTS_ERROR AS SELECT src, 'EAM_R5ACTCHECKLISTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_ACT), '0'), 38, 10) is null and 
                    src:ACK_ACT is not null) THEN
                    'ACK_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FOLLOWUPACT), '0'), 38, 10) is null and 
                    src:ACK_FOLLOWUPACT is not null) THEN
                    'ACK_FOLLOWUPACT contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FOLLOWUPACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROMJOBPLANREV), '0'), 38, 10) is null and 
                    src:ACK_FROMJOBPLANREV is not null) THEN
                    'ACK_FROMJOBPLANREV contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROMJOBPLANREV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROMPOINT), '0'), 38, 10) is null and 
                    src:ACK_FROMPOINT is not null) THEN
                    'ACK_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_HORIZONTAL_OFFSET is not null) THEN
                    'ACK_FROM_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:ACK_FROM_LATITUDE is not null) THEN
                    'ACK_FROM_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACK_FROM_LONGITUDE is not null) THEN
                    'ACK_FROM_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_OFFSET is not null) THEN
                    'ACK_FROM_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACK_FROM_OFFSET_PERCENTAGE is not null) THEN
                    'ACK_FROM_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ACK_FROM_REFERENCE is not null) THEN
                    'ACK_FROM_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_VERTICAL_OFFSET is not null) THEN
                    'ACK_FROM_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_FROM_XCOORDINATE is not null) THEN
                    'ACK_FROM_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_FROM_YCOORDINATE is not null) THEN
                    'ACK_FROM_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_FROM_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is null and 
                    src:ACK_LASTSAVED is not null) THEN
                    'ACK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACK_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_MAXSLIDERVALUE), '0'), 38, 10) is null and 
                    src:ACK_MAXSLIDERVALUE is not null) THEN
                    'ACK_MAXSLIDERVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_MAXSLIDERVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_MINSLIDERVALUE), '0'), 38, 10) is null and 
                    src:ACK_MINSLIDERVALUE is not null) THEN
                    'ACK_MINSLIDERVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_MINSLIDERVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_MOBILEDATEUPDATED), '1900-01-01')) is null and 
                    src:ACK_MOBILEDATEUPDATED is not null) THEN
                    'ACK_MOBILEDATEUPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACK_MOBILEDATEUPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_OCCURRENCE), '0'), 38, 10) is null and 
                    src:ACK_OCCURRENCE is not null) THEN
                    'ACK_OCCURRENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_OCCURRENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_ROUTESEQUENCE), '0'), 38, 10) is null and 
                    src:ACK_ROUTESEQUENCE is not null) THEN
                    'ACK_ROUTESEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_ROUTESEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_SEQUENCE), '0'), 38, 10) is null and 
                    src:ACK_SEQUENCE is not null) THEN
                    'ACK_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TOPOINT), '0'), 38, 10) is null and 
                    src:ACK_TOPOINT is not null) THEN
                    'ACK_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TOPOINT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_HORIZONTAL_OFFSET is not null) THEN
                    'ACK_TO_HORIZONTAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_HORIZONTAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:ACK_TO_LATITUDE is not null) THEN
                    'ACK_TO_LATITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_LATITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACK_TO_LONGITUDE is not null) THEN
                    'ACK_TO_LONGITUDE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_LONGITUDE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_OFFSET is not null) THEN
                    'ACK_TO_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACK_TO_OFFSET_PERCENTAGE is not null) THEN
                    'ACK_TO_OFFSET_PERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_OFFSET_PERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ACK_TO_REFERENCE is not null) THEN
                    'ACK_TO_REFERENCE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_REFERENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_VERTICAL_OFFSET is not null) THEN
                    'ACK_TO_VERTICAL_OFFSET contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_VERTICAL_OFFSET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_TO_XCOORDINATE is not null) THEN
                    'ACK_TO_XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_TO_YCOORDINATE is not null) THEN
                    'ACK_TO_YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_TO_YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACK_UPDATECOUNT is not null) THEN
                    'ACK_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_UPDATED), '1900-01-01')) is null and 
                    src:ACK_UPDATED is not null) THEN
                    'ACK_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACK_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_VALUE), '0'), 38, 10) is null and 
                    src:ACK_VALUE is not null) THEN
                    'ACK_VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ACK_VALUE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is null and 
                src:ACK_LASTSAVED is not null) THEN
                'ACK_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ACK_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ACK_CODE  ORDER BY 
            src:ACK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTCHECKLISTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_ACT), '0'), 38, 10) is null and 
                    src:ACK_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FOLLOWUPACT), '0'), 38, 10) is null and 
                    src:ACK_FOLLOWUPACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROMJOBPLANREV), '0'), 38, 10) is null and 
                    src:ACK_FROMJOBPLANREV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROMPOINT), '0'), 38, 10) is null and 
                    src:ACK_FROMPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_LATITUDE), '0'), 38, 10) is null and 
                    src:ACK_FROM_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACK_FROM_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACK_FROM_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_REFERENCE), '0'), 38, 10) is null and 
                    src:ACK_FROM_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_FROM_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_FROM_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_FROM_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_FROM_YCOORDINATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is null and 
                    src:ACK_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_MAXSLIDERVALUE), '0'), 38, 10) is null and 
                    src:ACK_MAXSLIDERVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_MINSLIDERVALUE), '0'), 38, 10) is null and 
                    src:ACK_MINSLIDERVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_MOBILEDATEUPDATED), '1900-01-01')) is null and 
                    src:ACK_MOBILEDATEUPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_OCCURRENCE), '0'), 38, 10) is null and 
                    src:ACK_OCCURRENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_ROUTESEQUENCE), '0'), 38, 10) is null and 
                    src:ACK_ROUTESEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_SEQUENCE), '0'), 38, 10) is null and 
                    src:ACK_SEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TOPOINT), '0'), 38, 10) is null and 
                    src:ACK_TOPOINT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_HORIZONTAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_LATITUDE), '0'), 38, 10) is null and 
                    src:ACK_TO_LATITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_LONGITUDE), '0'), 38, 10) is null and 
                    src:ACK_TO_LONGITUDE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is null and 
                    src:ACK_TO_OFFSET_PERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_REFERENCE), '0'), 38, 10) is null and 
                    src:ACK_TO_REFERENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_VERTICAL_OFFSET), '0'), 38, 10) is null and 
                    src:ACK_TO_VERTICAL_OFFSET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_XCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_TO_XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_TO_YCOORDINATE), '0'), 38, 10) is null and 
                    src:ACK_TO_YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ACK_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_UPDATED), '1900-01-01')) is null and 
                    src:ACK_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACK_VALUE), '0'), 38, 10) is null and 
                    src:ACK_VALUE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACK_LASTSAVED), '1900-01-01')) is null and 
                src:ACK_LASTSAVED is not null)