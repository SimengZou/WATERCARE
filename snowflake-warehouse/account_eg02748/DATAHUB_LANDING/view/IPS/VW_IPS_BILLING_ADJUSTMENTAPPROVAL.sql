CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ADJUSTMENTAPPROVAL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJUSTMENT::integer AS ADJUSTMENT, 
                        src:ADJUSTMENTAPPROVALKEY::integer AS ADJUSTMENTAPPROVALKEY, 
                        src:APPROVALCOMMENTS::integer AS APPROVALCOMMENTS, 
                        src:APPROVALDTTM::datetime AS APPROVALDTTM, 
                        src:APPROVALLEVELKEY::integer AS APPROVALLEVELKEY, 
                        src:APPROVEDBY::varchar AS APPROVEDBY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPLAYEDFLAG::varchar AS DISPLAYEDFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REJECTEDBY::varchar AS REJECTEDBY, 
                        src:REJECTIONCOMMENTS::integer AS REJECTIONCOMMENTS, 
                        src:REJECTIONDTTM::datetime AS REJECTIONDTTM, 
                        src:STATUS::varchar AS STATUS, 
                        src:SUBMISSIONCOMMENTS::integer AS SUBMISSIONCOMMENTS, 
                        src:SUBMISSIONDTTM::datetime AS SUBMISSIONDTTM, 
                        src:SUBMITTEDBY::varchar AS SUBMITTEDBY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:ADJUSTMENTAPPROVALKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADJUSTMENTAPPROVALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ADJUSTMENTAPPROVAL as strm))