CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ADDRTEMPLSERVICE_ERROR AS SELECT src, 'IPS_BILLING_ADDRTEMPLSERVICE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSTEMPLATEKEY is not null) THEN
                    'ADDRESSTEMPLATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSTEMPLATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRTEMPLSRVKEY), '0'), 38, 10) is null and 
                    src:ADDRTEMPLSRVKEY is not null) THEN
                    'ADDRTEMPLSRVKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRTEMPLSRVKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) THEN
                    'CONSUMPTIONPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CONSUMPTIONPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) THEN
                    'DISPLAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPERVIOUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:IMPERVIOUSPERCENTAGE is not null) THEN
                    'IMPERVIOUSPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:IMPERVIOUSPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) THEN
                    'SERVICEOPTIONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEOPTIONSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD1VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD1VALUE is not null) THEN
                    'SRVFLD1VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SRVFLD1VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD2VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD2VALUE is not null) THEN
                    'SRVFLD2VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SRVFLD2VALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD3VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD3VALUE is not null) THEN
                    'SRVFLD3VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:SRVFLD3VALUE) || '\'' WHEN 
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
                                    
                src:ADDRTEMPLSRVKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ADDRTEMPLSERVICE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSTEMPLATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRTEMPLSRVKEY), '0'), 38, 10) is null and 
                    src:ADDRTEMPLSRVKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is null and 
                    src:DISPLAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPERVIOUSPERCENTAGE), '0'), 38, 10) is null and 
                    src:IMPERVIOUSPERCENTAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD1VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD1VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD2VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD2VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVFLD3VALUE), '0'), 38, 10) is null and 
                    src:SRVFLD3VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)