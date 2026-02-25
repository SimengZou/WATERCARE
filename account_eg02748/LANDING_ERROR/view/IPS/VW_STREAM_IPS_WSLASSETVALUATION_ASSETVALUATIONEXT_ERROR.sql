CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETVALUATION_ASSETVALUATIONEXT_ERROR AS SELECT src, 'IPS_WSLASSETVALUATION_ASSETVALUATIONEXT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AQUISITIONDATE), '1900-01-01')) is null and 
                    src:AQUISITIONDATE is not null) THEN
                    'AQUISITIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:AQUISITIONDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETVALUATIONEXTKEY), '0'), 38, 10) is null and 
                    src:ASSETVALUATIONEXTKEY is not null) THEN
                    'ASSETVALUATIONEXTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETVALUATIONEXTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECIATIONMETHOD), '0'), 38, 10) is null and 
                    src:DEPRECIATIONMETHOD is not null) THEN
                    'DEPRECIATIONMETHOD contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECIATIONMETHOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTEDLIFE), '0'), 38, 10) is null and 
                    src:EXPECTEDLIFE is not null) THEN
                    'EXPECTEDLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPECTEDLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALCOST), '0'), 38, 10) is null and 
                    src:INITIALCOST is not null) THEN
                    'INITIALCOST contains a non-numeric value : \'' || AS_VARCHAR(src:INITIALCOST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:ASSETVALUATIONEXTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETVALUATION_ASSETVALUATIONEXT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AQUISITIONDATE), '1900-01-01')) is null and 
                    src:AQUISITIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETVALUATIONEXTKEY), '0'), 38, 10) is null and 
                    src:ASSETVALUATIONEXTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECIATIONMETHOD), '0'), 38, 10) is null and 
                    src:DEPRECIATIONMETHOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTEDLIFE), '0'), 38, 10) is null and 
                    src:EXPECTEDLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALCOST), '0'), 38, 10) is null and 
                    src:INITIALCOST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)