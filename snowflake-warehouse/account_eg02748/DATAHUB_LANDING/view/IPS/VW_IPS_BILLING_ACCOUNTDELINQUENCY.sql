CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCOUNTDELINQUENCY AS SELECT
                        src:ACCOUNTDELINQUENCYKEY::integer AS ACCOUNTDELINQUENCYKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSLIENKEY::integer AS ADDRESSLIENKEY, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COLLECTIONSKEY::integer AS COLLECTIONSKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DELINQUENCYENTRYAMOUNT::numeric(38, 10) AS DELINQUENCYENTRYAMOUNT, 
                        src:DELINQUENCYENTRYDATE::datetime AS DELINQUENCYENTRYDATE, 
                        src:DELINQUENCYLEVEL::varchar AS DELINQUENCYLEVEL, 
                        src:DELINQUENCYMANAGER::varchar AS DELINQUENCYMANAGER, 
                        src:DELINQUENCYOFFICER::varchar AS DELINQUENCYOFFICER, 
                        src:DELINQUENTTHROUGHDATE::datetime AS DELINQUENTTHROUGHDATE, 
                        src:EXTENSION::integer AS EXTENSION, 
                        src:HOLDFLAG::varchar AS HOLDFLAG, 
                        src:MILESTONEDUEDATE::datetime AS MILESTONEDUEDATE, 
                        src:MILESTONEENTRYDATE::datetime AS MILESTONEENTRYDATE, 
                        src:MILESTONEKEY::integer AS MILESTONEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTMILESTONEDATE::datetime AS NEXTMILESTONEDATE, 
                        src:PARCELLIENKEY::integer AS PARCELLIENKEY, 
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
    
                        
                src:ACCOUNTDELINQUENCYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTDELINQUENCYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCOUNTDELINQUENCY as strm))