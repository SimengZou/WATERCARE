CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_APPROVALLEVEL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROPTIONSPECIFIC::integer AS APPROPTIONSPECIFIC, 
                        src:APPROVALLEVELKEY::integer AS APPROVALLEVELKEY, 
                        src:APPROVALLEVELNAME::varchar AS APPROVALLEVELNAME, 
                        src:APPROVALOPTION::integer AS APPROVALOPTION, 
                        src:APPROVALOPTIONFORMULA::integer AS APPROVALOPTIONFORMULA, 
                        src:APPROVALSCHEME::integer AS APPROVALSCHEME, 
                        src:APPROVEUPTOAMOUNT::numeric(38, 10) AS APPROVEUPTOAMOUNT, 
                        src:BYPASSAMOUNT::numeric(38, 10) AS BYPASSAMOUNT, 
                        src:COSIGN::varchar AS COSIGN, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REJECTIONOPTION::integer AS REJECTIONOPTION, 
                        src:REJOPTIONSPECIFIC::integer AS REJOPTIONSPECIFIC, 
                        src:REJPTIONFORMULA::integer AS REJPTIONFORMULA, 
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
    
                        
                src:APPROVALLEVELKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPROVALLEVELKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_APPROVALLEVEL as strm))