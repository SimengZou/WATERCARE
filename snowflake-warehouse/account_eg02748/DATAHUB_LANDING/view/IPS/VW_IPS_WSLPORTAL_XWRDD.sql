CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLPORTAL_XWRDD AS SELECT
                        src:ACCTKEY::integer AS ACCTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DACTNAME::varchar AS DACTNAME, 
                        src:DDACCTNO::varchar AS DDACCTNO, 
                        src:DDACTYPE::varchar AS DDACTYPE, 
                        src:DDBANK::varchar AS DDBANK, 
                        src:DDBRANCH::varchar AS DDBRANCH, 
                        src:DDMASKNO::varchar AS DDMASKNO, 
                        src:DELETED::boolean AS DELETED, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PHONE::varchar AS PHONE, 
                        src:SDLDATE::datetime AS SDLDATE, 
                        src:SERVNO::integer AS SERVNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XWRDDKEY::integer AS XWRDDKEY, 
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
    
                        
                src:XWRDDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XWRDDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLPORTAL_XWRDD as strm))