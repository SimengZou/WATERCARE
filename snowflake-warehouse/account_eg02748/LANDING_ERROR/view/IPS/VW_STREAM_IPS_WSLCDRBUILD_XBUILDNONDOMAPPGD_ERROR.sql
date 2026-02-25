CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDNONDOMAPPGD_ERROR AS SELECT src, 'IPS_WSLCDRBUILD_XBUILDNONDOMAPPGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDAMOUNT), '0'), 38, 10) is null and 
                    src:ESTIMATEDAMOUNT is not null) THEN
                    'ESTIMATEDAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ESTIMATEDAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDQUANTITY), '0'), 38, 10) is null and 
                    src:ESTIMATEDQUANTITY is not null) THEN
                    'ESTIMATEDQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:ESTIMATEDQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDVALUE), '0'), 38, 10) is null and 
                    src:ESTIMATEDVALUE is not null) THEN
                    'ESTIMATEDVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:ESTIMATEDVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPECTIONNUMBER), '0'), 38, 10) is null and 
                    src:INSPECTIONNUMBER is not null) THEN
                    'INSPECTIONNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:INSPECTIONNUMBER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDFEESDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDFEESDPKEY is not null) THEN
                    'XBUILDFEESDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDFEESDPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDNONDOMAPPGDKEY), '0'), 38, 10) is null and 
                    src:XBUILDNONDOMAPPGDKEY is not null) THEN
                    'XBUILDNONDOMAPPGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDNONDOMAPPGDKEY) || '\'' WHEN 
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
                                    
                src:XBUILDNONDOMAPPGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDNONDOMAPPGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDAMOUNT), '0'), 38, 10) is null and 
                    src:ESTIMATEDAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDQUANTITY), '0'), 38, 10) is null and 
                    src:ESTIMATEDQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTIMATEDVALUE), '0'), 38, 10) is null and 
                    src:ESTIMATEDVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPECTIONNUMBER), '0'), 38, 10) is null and 
                    src:INSPECTIONNUMBER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDFEESDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDFEESDPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDNONDOMAPPGDKEY), '0'), 38, 10) is null and 
                    src:XBUILDNONDOMAPPGDKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)