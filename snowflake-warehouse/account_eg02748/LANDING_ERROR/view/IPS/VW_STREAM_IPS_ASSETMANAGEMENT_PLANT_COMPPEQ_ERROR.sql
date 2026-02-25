CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_PLANT_COMPPEQ_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_PLANT_COMPPEQ' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is null and 
                    src:AVGMONUSG is not null) THEN
                    'AVGMONUSG contains a non-numeric value : \'' || AS_VARCHAR(src:AVGMONUSG) || '\'' WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTLIFE), '0'), 38, 10) is null and 
                    src:EXPECTLIFE is not null) THEN
                    'EXPECTLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPECTLIFE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) THEN
                    'INSTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:INSTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is null and 
                    src:INTKEY is not null) THEN
                    'INTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is null and 
                    src:MFGDATE is not null) THEN
                    'MFGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MFGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) THEN
                    'MFGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MFGKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MTBF), '0'), 38, 10) is null and 
                    src:MTBF is not null) THEN
                    'MTBF contains a non-numeric value : \'' || AS_VARCHAR(src:MTBF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is null and 
                    src:PURCCOST is not null) THEN
                    'PURCCOST contains a non-numeric value : \'' || AS_VARCHAR(src:PURCCOST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is null and 
                    src:PURCDATE is not null) THEN
                    'PURCDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PURCDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) THEN
                    'SITEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SITEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBOFKEY), '0'), 38, 10) is null and 
                    src:SUBOFKEY is not null) THEN
                    'SUBOFKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SUBOFKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) THEN
                    'USGAREAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGAREAKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USGDATE), '1900-01-01')) is null and 
                    src:USGDATE is not null) THEN
                    'USGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:USGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is null and 
                    src:USGTOT is not null) THEN
                    'USGTOT contains a non-numeric value : \'' || AS_VARCHAR(src:USGTOT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARREXP), '1900-01-01')) is null and 
                    src:WARREXP is not null) THEN
                    'WARREXP contains a non-timestamp value : \'' || AS_VARCHAR(src:WARREXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WARRUSG), '0'), 38, 10) is null and 
                    src:WARRUSG is not null) THEN
                    'WARRUSG contains a non-numeric value : \'' || AS_VARCHAR(src:WARRUSG) || '\'' WHEN 
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
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_PLANT_COMPPEQ as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is null and 
                    src:AVGMONUSG is not null) or 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTLIFE), '0'), 38, 10) is null and 
                    src:EXPECTLIFE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is null and 
                    src:INTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is null and 
                    src:MFGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MTBF), '0'), 38, 10) is null and 
                    src:MTBF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is null and 
                    src:PURCCOST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is null and 
                    src:PURCDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBOFKEY), '0'), 38, 10) is null and 
                    src:SUBOFKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USGDATE), '1900-01-01')) is null and 
                    src:USGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is null and 
                    src:USGTOT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARREXP), '1900-01-01')) is null and 
                    src:WARREXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WARRUSG), '0'), 38, 10) is null and 
                    src:WARRUSG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is null and 
                    src:XCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is null and 
                    src:YCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is null and 
                    src:ZCOORD is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)