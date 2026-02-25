CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CRM_CUSTPROB_ERROR AS SELECT src, 'IPS_CRM_CUSTPROB' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTKEY), '0'), 38, 10) is null and 
                    src:ACCTKEY is not null) THEN
                    'ACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) THEN
                    'BUDGETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUDGETKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLERICDTTM), '1900-01-01')) is null and 
                    src:CLERICDTTM is not null) THEN
                    'CLERICDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CLERICDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is null and 
                    src:DISTFROMFT is not null) THEN
                    'DISTFROMFT contains a non-numeric value : \'' || AS_VARCHAR(src:DISTFROMFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is null and 
                    src:DISTFROMM is not null) THEN
                    'DISTFROMM contains a non-numeric value : \'' || AS_VARCHAR(src:DISTFROMM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is null and 
                    src:DISTTOFT is not null) THEN
                    'DISTTOFT contains a non-numeric value : \'' || AS_VARCHAR(src:DISTTOFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is null and 
                    src:DISTTOM is not null) THEN
                    'DISTTOM contains a non-numeric value : \'' || AS_VARCHAR(src:DISTTOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPACT), '0'), 38, 10) is null and 
                    src:IMPACT is not null) THEN
                    'IMPACT contains a non-numeric value : \'' || AS_VARCHAR(src:IMPACT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INCDTDTTM), '1900-01-01')) is null and 
                    src:INCDTDTTM is not null) THEN
                    'INCDTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INCDTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is null and 
                    src:INSPDAYS is not null) THEN
                    'INSPDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSPDTTM), '1900-01-01')) is null and 
                    src:INSPDTTM is not null) THEN
                    'INSPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INSPDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is null and 
                    src:INSPHRS is not null) THEN
                    'INSPHRS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is null and 
                    src:INSPMINS is not null) THEN
                    'INSPMINS contains a non-numeric value : \'' || AS_VARCHAR(src:INSPMINS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) THEN
                    'MOBILEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MOBILEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROB), '0'), 38, 10) is null and 
                    src:PROB is not null) THEN
                    'PROB contains a non-numeric value : \'' || AS_VARCHAR(src:PROB) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PROBDTTM), '1900-01-01')) is null and 
                    src:PROBDTTM is not null) THEN
                    'PROBDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:PROBDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) THEN
                    'PROPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTYCALLS), '0'), 38, 10) is null and 
                    src:QTYCALLS is not null) THEN
                    'QTYCALLS contains a non-numeric value : \'' || AS_VARCHAR(src:QTYCALLS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is null and 
                    src:RESDTTM is not null) THEN
                    'RESDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RESDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDUEDTTM), '1900-01-01')) is null and 
                    src:RESDUEDTTM is not null) THEN
                    'RESDUEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RESDUEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCDCMPDTTM), '1900-01-01')) is null and 
                    src:SCDCMPDTTM is not null) THEN
                    'SCDCMPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCDCMPDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHDUEDTTM), '1900-01-01')) is null and 
                    src:SCHDUEDTTM is not null) THEN
                    'SCHDUEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHDUEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) THEN
                    'SCHEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVLINK), '0'), 38, 10) is null and 
                    src:SERVLINK is not null) THEN
                    'SERVLINK contains a non-numeric value : \'' || AS_VARCHAR(src:SERVLINK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) THEN
                    'SERVNO contains a non-numeric value : \'' || AS_VARCHAR(src:SERVNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEVERITY), '0'), 38, 10) is null and 
                    src:SEVERITY is not null) THEN
                    'SEVERITY contains a non-numeric value : \'' || AS_VARCHAR(src:SEVERITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STDUEDTTM), '1900-01-01')) is null and 
                    src:STDUEDTTM is not null) THEN
                    'STDUEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STDUEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is null and 
                    src:SUSPDTTM is not null) THEN
                    'SUSPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is null and 
                    src:XCOORDINATE is not null) THEN
                    'XCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:XCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is null and 
                    src:YCOORDINATE is not null) THEN
                    'YCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:YCOORDINATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is null and 
                    src:ZCOORDINATE is not null) THEN
                    'ZCOORDINATE contains a non-numeric value : \'' || AS_VARCHAR(src:ZCOORDINATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:SERVNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_CUSTPROB as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTKEY), '0'), 38, 10) is null and 
                    src:ACCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLERICDTTM), '1900-01-01')) is null and 
                    src:CLERICDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is null and 
                    src:DISTFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is null and 
                    src:DISTFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is null and 
                    src:DISTTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is null and 
                    src:DISTTOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPACT), '0'), 38, 10) is null and 
                    src:IMPACT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INCDTDTTM), '1900-01-01')) is null and 
                    src:INCDTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is null and 
                    src:INSPDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSPDTTM), '1900-01-01')) is null and 
                    src:INSPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is null and 
                    src:INSPHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is null and 
                    src:INSPMINS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is null and 
                    src:MOBILEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROB), '0'), 38, 10) is null and 
                    src:PROB is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PROBDTTM), '1900-01-01')) is null and 
                    src:PROBDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTYCALLS), '0'), 38, 10) is null and 
                    src:QTYCALLS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is null and 
                    src:RESDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDUEDTTM), '1900-01-01')) is null and 
                    src:RESDUEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCDCMPDTTM), '1900-01-01')) is null and 
                    src:SCDCMPDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHDUEDTTM), '1900-01-01')) is null and 
                    src:SCHDUEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is null and 
                    src:SCHEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVLINK), '0'), 38, 10) is null and 
                    src:SERVLINK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is null and 
                    src:SERVNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEVERITY), '0'), 38, 10) is null and 
                    src:SEVERITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STDUEDTTM), '1900-01-01')) is null and 
                    src:STDUEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is null and 
                    src:SUSPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is null and 
                    src:XCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is null and 
                    src:YCOORDINATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is null and 
                    src:ZCOORDINATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)