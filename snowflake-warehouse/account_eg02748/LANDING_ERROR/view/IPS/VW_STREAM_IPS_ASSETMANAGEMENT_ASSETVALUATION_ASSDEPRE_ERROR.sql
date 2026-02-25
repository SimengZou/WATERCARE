CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASSVALDATE), '1900-01-01')) is null and 
                    src:ASSVALDATE is not null) THEN
                    'ASSVALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ASSVALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) THEN
                    'ASSVALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is null and 
                    src:BGTKEY is not null) THEN
                    'BGTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BGTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is null and 
                    src:CAPTLEXP is not null) THEN
                    'CAPTLEXP contains a non-numeric value : \'' || AS_VARCHAR(src:CAPTLEXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECAMT), '0'), 38, 10) is null and 
                    src:DEPRECAMT is not null) THEN
                    'DEPRECAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEPRECDATE), '1900-01-01')) is null and 
                    src:DEPRECDATE is not null) THEN
                    'DEPRECDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DEPRECDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECKEY), '0'), 38, 10) is null and 
                    src:DEPRECKEY is not null) THEN
                    'DEPRECKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECPERD), '0'), 38, 10) is null and 
                    src:DEPRECPERD is not null) THEN
                    'DEPRECPERD contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECPERD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is null and 
                    src:DEPRECRATE is not null) THEN
                    'DEPRECRATE contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is null and 
                    src:DEPRECTYPE is not null) THEN
                    'DEPRECTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is null and 
                    src:DEPRECYTD is not null) THEN
                    'DEPRECYTD contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECYTD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) THEN
                    'EFFECTIVEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is null and 
                    src:EXPLIFE is not null) THEN
                    'EXPLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPLIFE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDATE), '1900-01-01')) is null and 
                    src:JOURNALDATE is not null) THEN
                    'JOURNALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:JOURNALDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERIODADJDEPREC), '0'), 38, 10) is null and 
                    src:PERIODADJDEPREC is not null) THEN
                    'PERIODADJDEPREC contains a non-numeric value : \'' || AS_VARCHAR(src:PERIODADJDEPREC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLDEPKEY), '0'), 38, 10) is null and 
                    src:RECLDEPKEY is not null) THEN
                    'RECLDEPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RECLDEPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLNO), '0'), 38, 10) is null and 
                    src:RECLNO is not null) THEN
                    'RECLNO contains a non-numeric value : \'' || AS_VARCHAR(src:RECLNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is null and 
                    src:RESIDLIFE is not null) THEN
                    'RESIDLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:RESIDLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is null and 
                    src:RESIDVAL is not null) THEN
                    'RESIDVAL contains a non-numeric value : \'' || AS_VARCHAR(src:RESIDVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALFINALCOST), '0'), 38, 10) is null and 
                    src:REVALFINALCOST is not null) THEN
                    'REVALFINALCOST contains a non-numeric value : \'' || AS_VARCHAR(src:REVALFINALCOST) || '\'' WHEN 
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
                                    
                src:DEPRECKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as strm)
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ASSVALDATE), '1900-01-01')) is null and 
                    src:ASSVALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is null and 
                    src:BGTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is null and 
                    src:CAPTLEXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECAMT), '0'), 38, 10) is null and 
                    src:DEPRECAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEPRECDATE), '1900-01-01')) is null and 
                    src:DEPRECDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECKEY), '0'), 38, 10) is null and 
                    src:DEPRECKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECPERD), '0'), 38, 10) is null and 
                    src:DEPRECPERD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is null and 
                    src:DEPRECRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is null and 
                    src:DEPRECTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is null and 
                    src:DEPRECYTD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is null and 
                    src:EXPLIFE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDATE), '1900-01-01')) is null and 
                    src:JOURNALDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERIODADJDEPREC), '0'), 38, 10) is null and 
                    src:PERIODADJDEPREC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLDEPKEY), '0'), 38, 10) is null and 
                    src:RECLDEPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECLNO), '0'), 38, 10) is null and 
                    src:RECLNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is null and 
                    src:RESIDLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is null and 
                    src:RESIDVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALFINALCOST), '0'), 38, 10) is null and 
                    src:REVALFINALCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WRITEDNVAL), '0'), 38, 10) is null and 
                    src:WRITEDNVAL is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)