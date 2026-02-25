CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_APBILLACCTCHNGLOG AS SELECT
                        src:ACCTCHANGETRANSFERTYPE::integer AS ACCTCHANGETRANSFERTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBILLACCTCHNGLOGKEY::integer AS APBILLACCTCHNGLOGKEY, 
                        src:APKEY::integer AS APKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:FROMBILLACCTKEY::integer AS FROMBILLACCTKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:TOBILLACCTKEY::integer AS TOBILLACCTKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XFERTRNFLAG::varchar AS XFERTRNFLAG, 
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
    
                        
                src:APBILLACCTCHNGLOGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APBILLACCTCHNGLOGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_APBILLACCTCHNGLOG as strm))