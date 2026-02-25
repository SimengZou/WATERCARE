CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_REFUNDAPPROVAL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROVALBY::varchar AS APPROVALBY, 
                        src:APPROVALCOMMENTS::integer AS APPROVALCOMMENTS, 
                        src:APPROVALDTTM::datetime AS APPROVALDTTM, 
                        src:APPROVALLEVELKEY::integer AS APPROVALLEVELKEY, 
                        src:APPROVALSTATUS::varchar AS APPROVALSTATUS, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISPLAYEDFLAG::varchar AS DISPLAYEDFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REFUNDDETAILAPPROVALKEY::integer AS REFUNDDETAILAPPROVALKEY, 
                        src:REFUNDKEY::integer AS REFUNDKEY, 
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
    
                        
                src:REFUNDDETAILAPPROVALKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REFUNDDETAILAPPROVALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_REFUNDAPPROVAL as strm))