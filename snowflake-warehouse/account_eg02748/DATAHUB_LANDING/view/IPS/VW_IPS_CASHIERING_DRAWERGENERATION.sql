CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_DRAWERGENERATION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTOPOST::varchar AS AUTOPOST, 
                        src:BATCHDESC::varchar AS BATCHDESC, 
                        src:BATCHGROUP::varchar AS BATCHGROUP, 
                        src:BATCHSTAT::varchar AS BATCHSTAT, 
                        src:CASHIER::varchar AS CASHIER, 
                        src:CASHINAMT::numeric(38, 10) AS CASHINAMT, 
                        src:CASHLIMITAMT::numeric(38, 10) AS CASHLIMITAMT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEFAULTREGISTERGROUPKEY::integer AS DEFAULTREGISTERGROUPKEY, 
                        src:DRWRGENKEY::integer AS DRWRGENKEY, 
                        src:DRWRSTAT::varchar AS DRWRSTAT, 
                        src:DRWRTYPE::varchar AS DRWRTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OBGTNO::integer AS OBGTNO, 
                        src:REGKEY::integer AS REGKEY, 
                        src:SBGTNO::integer AS SBGTNO, 
                        src:SRC::varchar AS SRC, 
                        src:STARTTM::datetime AS STARTTM, 
                        src:STOPTM::datetime AS STOPTM, 
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
    
                        
                src:DRWRGENKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DRWRGENKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_DRAWERGENERATION as strm))