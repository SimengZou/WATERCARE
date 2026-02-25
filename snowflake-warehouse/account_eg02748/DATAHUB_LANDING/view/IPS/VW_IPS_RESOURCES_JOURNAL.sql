CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_JOURNAL AS SELECT
                        src:ACCOUNTINGPERIOD::integer AS ACCOUNTINGPERIOD, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:CREDITBUDGETNUMBERKEY::integer AS CREDITBUDGETNUMBERKEY, 
                        src:DEBITBUDGETNUMBERKEY::integer AS DEBITBUDGETNUMBERKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:FISCALYEAR::integer AS FISCALYEAR, 
                        src:JOURNALKEY::integer AS JOURNALKEY, 
                        src:JOURNALRUNKEY::integer AS JOURNALRUNKEY, 
                        src:JOURNALSUMMARYKEY::integer AS JOURNALSUMMARYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIDISTRIBUTIOLNALFLAG::varchar AS MULTIDISTRIBUTIOLNALFLAG, 
                        src:ORIGIN::varchar AS ORIGIN, 
                        src:TAXINCLUSIVEFLAG::varchar AS TAXINCLUSIVEFLAG, 
                        src:TRANSACTIONBY::varchar AS TRANSACTIONBY, 
                        src:TRANSACTIONDTTM::datetime AS TRANSACTIONDTTM, 
                        src:TRANSACTIONNUMBER::integer AS TRANSACTIONNUMBER, 
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
    
                        
                src:JOURNALKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JOURNALKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_JOURNAL as strm))