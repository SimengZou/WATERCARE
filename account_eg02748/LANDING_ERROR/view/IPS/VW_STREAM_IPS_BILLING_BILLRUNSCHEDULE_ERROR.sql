CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLRUNSCHEDULE_ERROR AS SELECT src, 'IPS_BILLING_BILLRUNSCHEDULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:BILLRUNSCHEDULEKEY is not null) THEN
                    'BILLRUNSCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNSCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCREMENTVALUE), '0'), 38, 10) is null and 
                    src:INCREMENTVALUE is not null) THEN
                    'INCREMENTVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:INCREMENTVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBERPRIOR), '0'), 38, 10) is null and 
                    src:NUMBERPRIOR is not null) THEN
                    'NUMBERPRIOR contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBERPRIOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORTIMEFRAME), '0'), 38, 10) is null and 
                    src:PRIORTIMEFRAME is not null) THEN
                    'PRIORTIMEFRAME contains a non-numeric value : \'' || AS_VARCHAR(src:PRIORTIMEFRAME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBSEQUENTRUNTYPE), '0'), 38, 10) is null and 
                    src:SUBSEQUENTRUNTYPE is not null) THEN
                    'SUBSEQUENTRUNTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:SUBSEQUENTRUNTYPE) || '\'' WHEN 
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
                                    
                src:BILLRUNSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLRUNSCHEDULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:BILLRUNSCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCREMENTVALUE), '0'), 38, 10) is null and 
                    src:INCREMENTVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBERPRIOR), '0'), 38, 10) is null and 
                    src:NUMBERPRIOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORTIMEFRAME), '0'), 38, 10) is null and 
                    src:PRIORTIMEFRAME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBSEQUENTRUNTYPE), '0'), 38, 10) is null and 
                    src:SUBSEQUENTRUNTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)