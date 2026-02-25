CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_REFUNDAPPROVAL_ERROR AS SELECT src, 'IPS_BILLING_REFUNDAPPROVAL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALCOMMENTS), '0'), 38, 10) is null and 
                    src:APPROVALCOMMENTS is not null) THEN
                    'APPROVALCOMMENTS contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALCOMMENTS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPROVALDTTM), '1900-01-01')) is null and 
                    src:APPROVALDTTM is not null) THEN
                    'APPROVALDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:APPROVALDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVELKEY), '0'), 38, 10) is null and 
                    src:APPROVALLEVELKEY is not null) THEN
                    'APPROVALLEVELKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALLEVELKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDDETAILAPPROVALKEY), '0'), 38, 10) is null and 
                    src:REFUNDDETAILAPPROVALKEY is not null) THEN
                    'REFUNDDETAILAPPROVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDDETAILAPPROVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) THEN
                    'REFUNDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBMISSIONCOMMENTS), '0'), 38, 10) is null and 
                    src:SUBMISSIONCOMMENTS is not null) THEN
                    'SUBMISSIONCOMMENTS contains a non-numeric value : \'' || AS_VARCHAR(src:SUBMISSIONCOMMENTS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUBMISSIONDTTM), '1900-01-01')) is null and 
                    src:SUBMISSIONDTTM is not null) THEN
                    'SUBMISSIONDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SUBMISSIONDTTM) || '\'' WHEN 
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
                                    
                src:REFUNDDETAILAPPROVALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_REFUNDAPPROVAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALCOMMENTS), '0'), 38, 10) is null and 
                    src:APPROVALCOMMENTS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPROVALDTTM), '1900-01-01')) is null and 
                    src:APPROVALDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVELKEY), '0'), 38, 10) is null and 
                    src:APPROVALLEVELKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDDETAILAPPROVALKEY), '0'), 38, 10) is null and 
                    src:REFUNDDETAILAPPROVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBMISSIONCOMMENTS), '0'), 38, 10) is null and 
                    src:SUBMISSIONCOMMENTS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUBMISSIONDTTM), '1900-01-01')) is null and 
                    src:SUBMISSIONDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)