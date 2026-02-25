CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTDELINQUENCY AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                                        
                src:ACCOUNTDELINQUENCYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTDELINQUENCY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSLIENKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COLLECTIONSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENCYENTRYAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DELINQUENCYENTRYDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DELINQUENTTHROUGHDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXTENSION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MILESTONEDUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MILESTONEENTRYDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NEXTMILESTONEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARCELLIENKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null