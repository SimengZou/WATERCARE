CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DELINQUENCYHISTORY AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSLIENKEY::integer AS ADDRESSLIENKEY, 
                        src:AUTODECANDRESPAYPOSTDTTM::datetime AS AUTODECANDRESPAYPOSTDTTM, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COLLECTIONSKEY::integer AS COLLECTIONSKEY, 
                        src:COLLECTIONSRUNKEY::integer AS COLLECTIONSRUNKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYENTRYAMOUNT::numeric(38, 10) AS DELINQUENCYENTRYAMOUNT, 
                        src:DELINQUENCYENTRYBALANCE::numeric(38, 10) AS DELINQUENCYENTRYBALANCE, 
                        src:DELINQUENCYENTRYDATE::datetime AS DELINQUENCYENTRYDATE, 
                        src:DELINQUENCYHISTORYKEY::integer AS DELINQUENCYHISTORYKEY, 
                        src:DELINQUENCYLEVEL::varchar AS DELINQUENCYLEVEL, 
                        src:DELINQUENCYMANAGER::varchar AS DELINQUENCYMANAGER, 
                        src:DELINQUENCYOFFICER::varchar AS DELINQUENCYOFFICER, 
                        src:DELINQUENCYRUNKEY::integer AS DELINQUENCYRUNKEY, 
                        src:DELINQUENTAMOUNT::numeric(38, 10) AS DELINQUENTAMOUNT, 
                        src:DELINQUENTTHROUGHDATE::datetime AS DELINQUENTTHROUGHDATE, 
                        src:DISPLAYHISTORYRECORD::varchar AS DISPLAYHISTORYRECORD, 
                        src:EXTENSION::integer AS EXTENSION, 
                        src:HOLDFLAG::varchar AS HOLDFLAG, 
                        src:MILESTONEDUEDATE::datetime AS MILESTONEDUEDATE, 
                        src:MILESTONEENTRYDATE::datetime AS MILESTONEENTRYDATE, 
                        src:MILESTONEENTRYREASON::varchar AS MILESTONEENTRYREASON, 
                        src:MILESTONEKEY::integer AS MILESTONEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PARCELLIENKEY::integer AS PARCELLIENKEY, 
                        src:PROCESSINGRUNKEY::integer AS PROCESSINGRUNKEY, 
                        src:RESOLUTION::varchar AS RESOLUTION, 
                        src:RESOLVEDBY::varchar AS RESOLVEDBY, 
                        src:RESOLVEDDATE::datetime AS RESOLVEDDATE, 
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
    
                        
                src:DELINQUENCYHISTORYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DELINQUENCYHISTORYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DELINQUENCYHISTORY as strm))