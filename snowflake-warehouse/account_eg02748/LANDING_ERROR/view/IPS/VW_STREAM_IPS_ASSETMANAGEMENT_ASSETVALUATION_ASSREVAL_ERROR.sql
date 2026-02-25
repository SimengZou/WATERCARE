CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSREVAL_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSREVAL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is null and 
                    src:ACCDEPREC is not null) THEN
                    'ACCDEPREC contains a non-numeric value : \'' || AS_VARCHAR(src:ACCDEPREC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) THEN
                    'ASSVALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELETED), '0'), 38, 10) is null and 
                    src:DELETED is not null) THEN
                    'DELETED contains a non-numeric value : \'' || AS_VARCHAR(src:DELETED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) THEN
                    'EFFECTIVEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDATE), '1900-01-01')) is null and 
                    src:JOURNALDATE is not null) THEN
                    'JOURNALDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:JOURNALDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALVALUATIONRUN), '0'), 38, 10) is null and 
                    src:JOURNALVALUATIONRUN is not null) THEN
                    'JOURNALVALUATIONRUN contains a non-numeric value : \'' || AS_VARCHAR(src:JOURNALVALUATIONRUN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MARKETVAL), '0'), 38, 10) is null and 
                    src:MARKETVAL is not null) THEN
                    'MARKETVAL contains a non-numeric value : \'' || AS_VARCHAR(src:MARKETVAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWDEPRATE), '0'), 38, 10) is null and 
                    src:NEWDEPRATE is not null) THEN
                    'NEWDEPRATE contains a non-numeric value : \'' || AS_VARCHAR(src:NEWDEPRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWEXPLIFE), '0'), 38, 10) is null and 
                    src:NEWEXPLIFE is not null) THEN
                    'NEWEXPLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:NEWEXPLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWRESDVAL), '0'), 38, 10) is null and 
                    src:NEWRESDVAL is not null) THEN
                    'NEWRESDVAL contains a non-numeric value : \'' || AS_VARCHAR(src:NEWRESDVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPLCCOST), '0'), 38, 10) is null and 
                    src:REPLCCOST is not null) THEN
                    'REPLCCOST contains a non-numeric value : \'' || AS_VARCHAR(src:REPLCCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALKEY), '0'), 38, 10) is null and 
                    src:REVALKEY is not null) THEN
                    'REVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALMTHD), '0'), 38, 10) is null and 
                    src:REVALMTHD is not null) THEN
                    'REVALMTHD contains a non-numeric value : \'' || AS_VARCHAR(src:REVALMTHD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDTTM), '1900-01-01')) is null and 
                    src:VALDTTM is not null) THEN
                    'VALDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:VALDTTM) || '\'' WHEN 
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
                                    
                src:REVALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSREVAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is null and 
                    src:ACCDEPREC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELETED), '0'), 38, 10) is null and 
                    src:DELETED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDATE), '1900-01-01')) is null and 
                    src:JOURNALDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALVALUATIONRUN), '0'), 38, 10) is null and 
                    src:JOURNALVALUATIONRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MARKETVAL), '0'), 38, 10) is null and 
                    src:MARKETVAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWDEPRATE), '0'), 38, 10) is null and 
                    src:NEWDEPRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWEXPLIFE), '0'), 38, 10) is null and 
                    src:NEWEXPLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEWRESDVAL), '0'), 38, 10) is null and 
                    src:NEWRESDVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REPLCCOST), '0'), 38, 10) is null and 
                    src:REPLCCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALKEY), '0'), 38, 10) is null and 
                    src:REVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALMTHD), '0'), 38, 10) is null and 
                    src:REVALMTHD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDTTM), '1900-01-01')) is null and 
                    src:VALDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)