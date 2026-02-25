CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPSMN_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_SEWER_COMPSMN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) THEN
                    'BUDGETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUDGETKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is null and 
                    src:COMPLEXKEY is not null) THEN
                    'COMPLEXKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPLEXKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSGNFLOW), '0'), 38, 10) is null and 
                    src:DSGNFLOW is not null) THEN
                    'DSGNFLOW contains a non-numeric value : \'' || AS_VARCHAR(src:DSGNFLOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DWNDPTH), '0'), 38, 10) is null and 
                    src:DWNDPTH is not null) THEN
                    'DWNDPTH contains a non-numeric value : \'' || AS_VARCHAR(src:DWNDPTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DWNELEV), '0'), 38, 10) is null and 
                    src:DWNELEV is not null) THEN
                    'DWNELEV contains a non-numeric value : \'' || AS_VARCHAR(src:DWNELEV) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FFACTOR), '0'), 38, 10) is null and 
                    src:FFACTOR is not null) THEN
                    'FFACTOR contains a non-numeric value : \'' || AS_VARCHAR(src:FFACTOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is null and 
                    src:GISSTATIC is not null) THEN
                    'GISSTATIC contains a non-numeric value : \'' || AS_VARCHAR(src:GISSTATIC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUNDWAT), '0'), 38, 10) is null and 
                    src:GROUNDWAT is not null) THEN
                    'GROUNDWAT contains a non-numeric value : \'' || AS_VARCHAR(src:GROUNDWAT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) THEN
                    'INSTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:INSTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JTLEN), '0'), 38, 10) is null and 
                    src:JTLEN is not null) THEN
                    'JTLEN contains a non-numeric value : \'' || AS_VARCHAR(src:JTLEN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP1), '0'), 38, 10) is null and 
                    src:MAINCOMP1 is not null) THEN
                    'MAINCOMP1 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINCOMP1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP2), '0'), 38, 10) is null and 
                    src:MAINCOMP2 is not null) THEN
                    'MAINCOMP2 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINCOMP2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY1), '0'), 38, 10) is null and 
                    src:MAINKEY1 is not null) THEN
                    'MAINKEY1 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINKEY1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY2), '0'), 38, 10) is null and 
                    src:MAINKEY2 is not null) THEN
                    'MAINKEY2 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINKEY2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) THEN
                    'MFGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MFGKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEDIAM), '0'), 38, 10) is null and 
                    src:PIPEDIAM is not null) THEN
                    'PIPEDIAM contains a non-numeric value : \'' || AS_VARCHAR(src:PIPEDIAM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEHT), '0'), 38, 10) is null and 
                    src:PIPEHT is not null) THEN
                    'PIPEHT contains a non-numeric value : \'' || AS_VARCHAR(src:PIPEHT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPELEN), '0'), 38, 10) is null and 
                    src:PIPELEN is not null) THEN
                    'PIPELEN contains a non-numeric value : \'' || AS_VARCHAR(src:PIPELEN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is null and 
                    src:SEGKEY is not null) THEN
                    'SEGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SEGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) THEN
                    'SITEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SITEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SLP), '0'), 38, 10) is null and 
                    src:SLP is not null) THEN
                    'SLP contains a non-numeric value : \'' || AS_VARCHAR(src:SLP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPSDPTH), '0'), 38, 10) is null and 
                    src:UPSDPTH is not null) THEN
                    'UPSDPTH contains a non-numeric value : \'' || AS_VARCHAR(src:UPSDPTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPSELEV), '0'), 38, 10) is null and 
                    src:UPSELEV is not null) THEN
                    'UPSELEV contains a non-numeric value : \'' || AS_VARCHAR(src:UPSELEV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) THEN
                    'USGAREAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGAREAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is null and 
                    src:XCOORD is not null) THEN
                    'XCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:XCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is null and 
                    src:YCOORD is not null) THEN
                    'YCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:YCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is null and 
                    src:ZCOORD is not null) THEN
                    'ZCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:ZCOORD) || '\'' WHEN 
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
                                    
                src:COMPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPSMN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is null and 
                    src:COMPLEXKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSGNFLOW), '0'), 38, 10) is null and 
                    src:DSGNFLOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DWNDPTH), '0'), 38, 10) is null and 
                    src:DWNDPTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DWNELEV), '0'), 38, 10) is null and 
                    src:DWNELEV is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FFACTOR), '0'), 38, 10) is null and 
                    src:FFACTOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is null and 
                    src:GISSTATIC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUNDWAT), '0'), 38, 10) is null and 
                    src:GROUNDWAT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JTLEN), '0'), 38, 10) is null and 
                    src:JTLEN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP1), '0'), 38, 10) is null and 
                    src:MAINCOMP1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP2), '0'), 38, 10) is null and 
                    src:MAINCOMP2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY1), '0'), 38, 10) is null and 
                    src:MAINKEY1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY2), '0'), 38, 10) is null and 
                    src:MAINKEY2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEDIAM), '0'), 38, 10) is null and 
                    src:PIPEDIAM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPEHT), '0'), 38, 10) is null and 
                    src:PIPEHT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PIPELEN), '0'), 38, 10) is null and 
                    src:PIPELEN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is null and 
                    src:SEGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SLP), '0'), 38, 10) is null and 
                    src:SLP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPSDPTH), '0'), 38, 10) is null and 
                    src:UPSDPTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPSELEV), '0'), 38, 10) is null and 
                    src:UPSELEV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is null and 
                    src:XCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is null and 
                    src:YCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is null and 
                    src:ZCOORD is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)