CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_BILLTYPEDIRECTDEBITSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLTYPEDIRECTDEBITSUKEY::integer AS BILLTYPEDIRECTDEBITSUKEY, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COMBINEAMOUNT::varchar AS COMBINEAMOUNT, 
                        src:DELETED::boolean AS DELETED, 
                        src:EXTRACTAMOUNTCRITERIA::varchar AS EXTRACTAMOUNTCRITERIA, 
                        src:INCLUDEUNBILLED::varchar AS INCLUDEUNBILLED, 
                        src:MAXAMOUNT::numeric(38, 10) AS MAXAMOUNT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REFBALTHRUEXTRDATE::varchar AS REFBALTHRUEXTRDATE, 
                        src:REFRESHEXTRACTAMOUNT::varchar AS REFRESHEXTRACTAMOUNT, 
                        src:USEDDSETUPDEFAULT::varchar AS USEDDSETUPDEFAULT, 
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
    
                        
                src:BILLTYPEDIRECTDEBITSUKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BILLTYPEDIRECTDEBITSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_BILLTYPEDIRECTDEBITSETUP as strm))