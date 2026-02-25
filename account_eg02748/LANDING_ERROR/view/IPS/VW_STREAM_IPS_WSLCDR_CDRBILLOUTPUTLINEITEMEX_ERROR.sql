CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDR_CDRBILLOUTPUTLINEITEMEX_ERROR AS SELECT src, 'IPS_WSLCDR_CDRBILLOUTPUTLINEITEMEX' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNNUMBER), '0'), 38, 10) is null and 
                    src:BILLRUNNUMBER is not null) THEN
                    'BILLRUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBILLOUTPUTLINEITEMEXKEY), '0'), 38, 10) is null and 
                    src:CDRBILLOUTPUTLINEITEMEXKEY is not null) THEN
                    'CDRBILLOUTPUTLINEITEMEXKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRBILLOUTPUTLINEITEMEXKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) THEN
                    'FEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) THEN
                    'QTY contains a non-numeric value : \'' || AS_VARCHAR(src:QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALAMOUNT), '0'), 38, 10) is null and 
                    src:TOTALAMOUNT is not null) THEN
                    'TOTALAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) THEN
                    'VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
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
                                    
                src:CDRBILLOUTPUTLINEITEMEXKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDR_CDRBILLOUTPUTLINEITEMEX as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNNUMBER), '0'), 38, 10) is null and 
                    src:BILLRUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBILLOUTPUTLINEITEMEXKEY), '0'), 38, 10) is null and 
                    src:CDRBILLOUTPUTLINEITEMEXKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QTY), '0'), 38, 10) is null and 
                    src:QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALAMOUNT), '0'), 38, 10) is null and 
                    src:TOTALAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)