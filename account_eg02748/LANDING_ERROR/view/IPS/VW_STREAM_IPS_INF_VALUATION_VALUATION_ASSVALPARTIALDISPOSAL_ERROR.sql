CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL_ERROR AS SELECT src, 'IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALKEY is not null) THEN
                    'ASSVALPARTIALDISPOSALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPOSALDATE), '1900-01-01')) is null and 
                    src:DISPOSALDATE is not null) THEN
                    'DISPOSALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISPOSALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALPERCENTAGE), '0'), 38, 10) is null and 
                    src:DISPOSALPERCENTAGE is not null) THEN
                    'DISPOSALPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DISPOSALPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALQUANTITY), '0'), 38, 10) is null and 
                    src:DISPOSALQUANTITY is not null) THEN
                    'DISPOSALQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:DISPOSALQUANTITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENINGQUANTITY), '0'), 38, 10) is null and 
                    src:OPENINGQUANTITY is not null) THEN
                    'OPENINGQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:OPENINGQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROCEEDSONDISPOSAL), '0'), 38, 10) is null and 
                    src:PROCEEDSONDISPOSAL is not null) THEN
                    'PROCEEDSONDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:PROCEEDSONDISPOSAL) || '\'' WHEN 
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
                                    
                src:ASSVALPARTIALDISPOSALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPOSALDATE), '1900-01-01')) is null and 
                    src:DISPOSALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALPERCENTAGE), '0'), 38, 10) is null and 
                    src:DISPOSALPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALQUANTITY), '0'), 38, 10) is null and 
                    src:DISPOSALQUANTITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENINGQUANTITY), '0'), 38, 10) is null and 
                    src:OPENINGQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROCEEDSONDISPOSAL), '0'), 38, 10) is null and 
                    src:PROCEEDSONDISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)