CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD_ERROR AS SELECT src, 'IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECAFTERDISPOSAL is not null) THEN
                    'ACCUMDEPRECAFTERDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:ACCUMDEPRECAFTERDISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECAMOUNT), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECAMOUNT is not null) THEN
                    'ACCUMDEPRECAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ACCUMDEPRECAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECBEFOREDISPOSAL), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECBEFOREDISPOSAL is not null) THEN
                    'ACCUMDEPRECBEFOREDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:ACCUMDEPRECBEFOREDISPOSAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALGDKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALGDKEY is not null) THEN
                    'ASSVALPARTIALDISPOSALGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALPARTIALDISPOSALGDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALKEY is not null) THEN
                    'ASSVALPARTIALDISPOSALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:CURRENTCOSTAFTERDISPOSAL is not null) THEN
                    'CURRENTCOSTAFTERDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTCOSTAFTERDISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTB4DISPOSAL), '0'), 38, 10) is null and 
                    src:CURRENTCOSTB4DISPOSAL is not null) THEN
                    'CURRENTCOSTB4DISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTCOSTB4DISPOSAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPOSALDATE), '1900-01-01')) is null and 
                    src:DISPOSALDATE is not null) THEN
                    'DISPOSALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISPOSALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALNUMBER), '0'), 38, 10) is null and 
                    src:DISPOSALNUMBER is not null) THEN
                    'DISPOSALNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPOSALNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALVALUE), '0'), 38, 10) is null and 
                    src:DISPOSALVALUE is not null) THEN
                    'DISPOSALVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:DISPOSALVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENINGQUANTITY), '0'), 38, 10) is null and 
                    src:OPENINGQUANTITY is not null) THEN
                    'OPENINGQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:OPENINGQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:PERCENTAGEAFTERDISPOSAL is not null) THEN
                    'PERCENTAGEAFTERDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:PERCENTAGEAFTERDISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEB4DISPOSAL), '0'), 38, 10) is null and 
                    src:PERCENTAGEB4DISPOSAL is not null) THEN
                    'PERCENTAGEB4DISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:PERCENTAGEB4DISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEDISPOSED), '0'), 38, 10) is null and 
                    src:PERCENTAGEDISPOSED is not null) THEN
                    'PERCENTAGEDISPOSED contains a non-numeric value : \'' || AS_VARCHAR(src:PERCENTAGEDISPOSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is null and 
                    src:PROFITLOSS is not null) THEN
                    'PROFITLOSS contains a non-numeric value : \'' || AS_VARCHAR(src:PROFITLOSS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:QUANTITYAFTERDISPOSAL is not null) THEN
                    'QUANTITYAFTERDISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYAFTERDISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYB4DISPOSAL), '0'), 38, 10) is null and 
                    src:QUANTITYB4DISPOSAL is not null) THEN
                    'QUANTITYB4DISPOSAL contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYB4DISPOSAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYDISPOSED), '0'), 38, 10) is null and 
                    src:QUANTITYDISPOSED is not null) THEN
                    'QUANTITYDISPOSED contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITYDISPOSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUATIONKEY), '0'), 38, 10) is null and 
                    src:VALUATIONKEY is not null) THEN
                    'VALUATIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VALUATIONKEY) || '\'' WHEN 
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
                                    
                src:ASSVALPARTIALDISPOSALGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECAFTERDISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECAMOUNT), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECBEFOREDISPOSAL), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECBEFOREDISPOSAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALGDKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALGDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALPARTIALDISPOSALKEY), '0'), 38, 10) is null and 
                    src:ASSVALPARTIALDISPOSALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:CURRENTCOSTAFTERDISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTB4DISPOSAL), '0'), 38, 10) is null and 
                    src:CURRENTCOSTB4DISPOSAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPOSALDATE), '1900-01-01')) is null and 
                    src:DISPOSALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALNUMBER), '0'), 38, 10) is null and 
                    src:DISPOSALNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALVALUE), '0'), 38, 10) is null and 
                    src:DISPOSALVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENINGQUANTITY), '0'), 38, 10) is null and 
                    src:OPENINGQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:PERCENTAGEAFTERDISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEB4DISPOSAL), '0'), 38, 10) is null and 
                    src:PERCENTAGEB4DISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTAGEDISPOSED), '0'), 38, 10) is null and 
                    src:PERCENTAGEDISPOSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is null and 
                    src:PROFITLOSS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYAFTERDISPOSAL), '0'), 38, 10) is null and 
                    src:QUANTITYAFTERDISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYB4DISPOSAL), '0'), 38, 10) is null and 
                    src:QUANTITYB4DISPOSAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITYDISPOSED), '0'), 38, 10) is null and 
                    src:QUANTITYDISPOSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUATIONKEY), '0'), 38, 10) is null and 
                    src:VALUATIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)