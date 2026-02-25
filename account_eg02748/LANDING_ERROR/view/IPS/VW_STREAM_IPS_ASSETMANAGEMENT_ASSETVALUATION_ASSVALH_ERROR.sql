CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALH_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALH' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is null and 
                    src:ACCDEPREC is not null) THEN
                    'ACCDEPREC contains a non-numeric value : \'' || AS_VARCHAR(src:ACCDEPREC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCPERIOD), '0'), 38, 10) is null and 
                    src:ACCPERIOD is not null) THEN
                    'ACCPERIOD contains a non-numeric value : \'' || AS_VARCHAR(src:ACCPERIOD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACQDATE), '1900-01-01')) is null and 
                    src:ACQDATE is not null) THEN
                    'ACQDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ACQDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALHISTKEY), '0'), 38, 10) is null and 
                    src:ASSVALHISTKEY is not null) THEN
                    'ASSVALHISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALHISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) THEN
                    'ASSVALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is null and 
                    src:BGTKEY is not null) THEN
                    'BGTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BGTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY2), '0'), 38, 10) is null and 
                    src:BGTKEY2 is not null) THEN
                    'BGTKEY2 contains a non-numeric value : \'' || AS_VARCHAR(src:BGTKEY2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY3), '0'), 38, 10) is null and 
                    src:BGTKEY3 is not null) THEN
                    'BGTKEY3 contains a non-numeric value : \'' || AS_VARCHAR(src:BGTKEY3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPTLDATE), '1900-01-01')) is null and 
                    src:CAPTLDATE is not null) THEN
                    'CAPTLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CAPTLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is null and 
                    src:CAPTLEXP is not null) THEN
                    'CAPTLEXP contains a non-numeric value : \'' || AS_VARCHAR(src:CAPTLEXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is null and 
                    src:DEPRECRATE is not null) THEN
                    'DEPRECRATE contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is null and 
                    src:DEPRECTYPE is not null) THEN
                    'DEPRECTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is null and 
                    src:DEPRECYTD is not null) THEN
                    'DEPRECYTD contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECYTD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRESIDLF), '0'), 38, 10) is null and 
                    src:DEPRESIDLF is not null) THEN
                    'DEPRESIDLF contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRESIDLF) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISJOURNALDATE), '1900-01-01')) is null and 
                    src:DISJOURNALDATE is not null) THEN
                    'DISJOURNALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISJOURNALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPBGTKEY), '0'), 38, 10) is null and 
                    src:DISPBGTKEY is not null) THEN
                    'DISPBGTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DISPBGTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is null and 
                    src:DISPDATE is not null) THEN
                    'DISPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPVAL), '0'), 38, 10) is null and 
                    src:DISPVAL is not null) THEN
                    'DISPVAL contains a non-numeric value : \'' || AS_VARCHAR(src:DISPVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is null and 
                    src:EXPLIFE is not null) THEN
                    'EXPLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITCOST), '0'), 38, 10) is null and 
                    src:INITCOST is not null) THEN
                    'INITCOST contains a non-numeric value : \'' || AS_VARCHAR(src:INITCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSUREVAL), '0'), 38, 10) is null and 
                    src:INSUREVAL is not null) THEN
                    'INSUREVAL contains a non-numeric value : \'' || AS_VARCHAR(src:INSUREVAL) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINTEXP), '0'), 38, 10) is null and 
                    src:MAINTEXP is not null) THEN
                    'MAINTEXP contains a non-numeric value : \'' || AS_VARCHAR(src:MAINTEXP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is null and 
                    src:PROFITLOSS is not null) THEN
                    'PROFITLOSS contains a non-numeric value : \'' || AS_VARCHAR(src:PROFITLOSS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLNO), '0'), 38, 10) is null and 
                    src:RECLNO is not null) THEN
                    'RECLNO contains a non-numeric value : \'' || AS_VARCHAR(src:RECLNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is null and 
                    src:RESIDLIFE is not null) THEN
                    'RESIDLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:RESIDLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is null and 
                    src:RESIDVAL is not null) THEN
                    'RESIDVAL contains a non-numeric value : \'' || AS_VARCHAR(src:RESIDVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALAMT), '0'), 38, 10) is null and 
                    src:REVALAMT is not null) THEN
                    'REVALAMT contains a non-numeric value : \'' || AS_VARCHAR(src:REVALAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is null and 
                    src:RWATTRKEY is not null) THEN
                    'RWATTRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RWATTRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXVAL), '0'), 38, 10) is null and 
                    src:TAXVAL is not null) THEN
                    'TAXVAL contains a non-numeric value : \'' || AS_VARCHAR(src:TAXVAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TOBESOLDDATE), '1900-01-01')) is null and 
                    src:TOBESOLDDATE is not null) THEN
                    'TOBESOLDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TOBESOLDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDATE), '1900-01-01')) is null and 
                    src:VALDATE is not null) THEN
                    'VALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:VALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WRITEDNVAL), '0'), 38, 10) is null and 
                    src:WRITEDNVAL is not null) THEN
                    'WRITEDNVAL contains a non-numeric value : \'' || AS_VARCHAR(src:WRITEDNVAL) || '\'' WHEN 
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
                                    
                src:ASSVALHISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALH as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is null and 
                    src:ACCDEPREC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCPERIOD), '0'), 38, 10) is null and 
                    src:ACCPERIOD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACQDATE), '1900-01-01')) is null and 
                    src:ACQDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALHISTKEY), '0'), 38, 10) is null and 
                    src:ASSVALHISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is null and 
                    src:BGTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY2), '0'), 38, 10) is null and 
                    src:BGTKEY2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY3), '0'), 38, 10) is null and 
                    src:BGTKEY3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPTLDATE), '1900-01-01')) is null and 
                    src:CAPTLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is null and 
                    src:CAPTLEXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is null and 
                    src:DEPRECRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is null and 
                    src:DEPRECTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is null and 
                    src:DEPRECYTD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRESIDLF), '0'), 38, 10) is null and 
                    src:DEPRESIDLF is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISJOURNALDATE), '1900-01-01')) is null and 
                    src:DISJOURNALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPBGTKEY), '0'), 38, 10) is null and 
                    src:DISPBGTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISPDATE), '1900-01-01')) is null and 
                    src:DISPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPVAL), '0'), 38, 10) is null and 
                    src:DISPVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is null and 
                    src:EXPLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITCOST), '0'), 38, 10) is null and 
                    src:INITCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSUREVAL), '0'), 38, 10) is null and 
                    src:INSUREVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is null and 
                    src:LINEARFROMFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is null and 
                    src:LINEARFROMM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is null and 
                    src:LINEARTOFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is null and 
                    src:LINEARTOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINTEXP), '0'), 38, 10) is null and 
                    src:MAINTEXP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROFITLOSS), '0'), 38, 10) is null and 
                    src:PROFITLOSS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLNO), '0'), 38, 10) is null and 
                    src:RECLNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is null and 
                    src:RESIDLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is null and 
                    src:RESIDVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALAMT), '0'), 38, 10) is null and 
                    src:REVALAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is null and 
                    src:RWATTRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXVAL), '0'), 38, 10) is null and 
                    src:TAXVAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TOBESOLDDATE), '1900-01-01')) is null and 
                    src:TOBESOLDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDATE), '1900-01-01')) is null and 
                    src:VALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WRITEDNVAL), '0'), 38, 10) is null and 
                    src:WRITEDNVAL is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)