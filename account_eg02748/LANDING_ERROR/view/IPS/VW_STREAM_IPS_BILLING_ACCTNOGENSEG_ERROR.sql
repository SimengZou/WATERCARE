CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCTNOGENSEG_ERROR AS SELECT src, 'IPS_BILLING_ACCTNOGENSEG' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTNOGENSEGKEY), '0'), 38, 10) is null and 
                    src:ACCTNOGENSEGKEY is not null) THEN
                    'ACCTNOGENSEGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTNOGENSEGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTNOGENSETUPKEY), '0'), 38, 10) is null and 
                    src:ACCTNOGENSETUPKEY is not null) THEN
                    'ACCTNOGENSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTNOGENSETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTVALUE), '0'), 38, 10) is null and 
                    src:CURRENTVALUE is not null) THEN
                    'CURRENTVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYOWNERAS), '0'), 38, 10) is null and 
                    src:DISPLAYOWNERAS is not null) THEN
                    'DISPLAYOWNERAS contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYOWNERAS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NODIGITS), '0'), 38, 10) is null and 
                    src:NODIGITS is not null) THEN
                    'NODIGITS contains a non-numeric value : \'' || AS_VARCHAR(src:NODIGITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGMENTNUMBER), '0'), 38, 10) is null and 
                    src:SEGMENTNUMBER is not null) THEN
                    'SEGMENTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:SEGMENTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTAT), '0'), 38, 10) is null and 
                    src:STARTAT is not null) THEN
                    'STARTAT contains a non-numeric value : \'' || AS_VARCHAR(src:STARTAT) || '\'' WHEN 
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
                                    
                src:ACCTNOGENSEGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCTNOGENSEG as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTNOGENSEGKEY), '0'), 38, 10) is null and 
                    src:ACCTNOGENSEGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTNOGENSETUPKEY), '0'), 38, 10) is null and 
                    src:ACCTNOGENSETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTVALUE), '0'), 38, 10) is null and 
                    src:CURRENTVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYOWNERAS), '0'), 38, 10) is null and 
                    src:DISPLAYOWNERAS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NODIGITS), '0'), 38, 10) is null and 
                    src:NODIGITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGMENTNUMBER), '0'), 38, 10) is null and 
                    src:SEGMENTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTAT), '0'), 38, 10) is null and 
                    src:STARTAT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)