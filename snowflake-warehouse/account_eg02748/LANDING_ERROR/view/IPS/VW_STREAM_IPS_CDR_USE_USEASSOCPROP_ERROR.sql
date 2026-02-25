CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEASSOCPROP_ERROR AS SELECT src, 'IPS_CDR_USE_USEASSOCPROP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEASSOCPROPKEY), '0'), 38, 10) is null and 
                    src:APUSEASSOCPROPKEY is not null) THEN
                    'APUSEASSOCPROPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEASSOCPROPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is null and 
                    src:APUSEKEY is not null) THEN
                    'APUSEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) THEN
                    'ENDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) THEN
                    'GROUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GROUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) THEN
                    'LINEARFROMFT contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARFROMFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) THEN
                    'LINEARFROMM contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARFROMM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) THEN
                    'LINEARTOFT contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARTOFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) THEN
                    'LINEARTOM contains a non-numeric value : \'' || AS_VARCHAR(src:LINEARTOM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) THEN
                    'PROPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is null and 
                    src:PROPOCCUPKEY is not null) THEN
                    'PROPOCCUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPOCCUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPE), '0'), 38, 10) is null and 
                    src:TYPE is not null) THEN
                    'TYPE contains a non-numeric value : \'' || AS_VARCHAR(src:TYPE) || '\'' WHEN 
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
                                    
                src:APUSEASSOCPROPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_USE_USEASSOCPROP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEASSOCPROPKEY), '0'), 38, 10) is null and 
                    src:APUSEASSOCPROPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is null and 
                    src:APUSEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROUPKEY), '0'), 38, 10) is null and 
                    src:GROUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is null and 
                    src:PROPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPOCCUPKEY), '0'), 38, 10) is null and 
                    src:PROPOCCUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPE), '0'), 38, 10) is null and 
                    src:TYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)