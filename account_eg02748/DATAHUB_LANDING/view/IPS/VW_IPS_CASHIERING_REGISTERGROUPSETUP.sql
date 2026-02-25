CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_REGISTERGROUPSETUP AS SELECT
                        src:ACCESSID::integer AS ACCESSID, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BMPFILE::varchar AS BMPFILE, 
                        src:CASHLIMITAMT::numeric(38, 10) AS CASHLIMITAMT, 
                        src:CASHSETUP::integer AS CASHSETUP, 
                        src:CHECKSETUP::integer AS CHECKSETUP, 
                        src:CREDITSETUP::integer AS CREDITSETUP, 
                        src:DEBITSETUP::integer AS DEBITSETUP, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:ENDORSE::varchar AS ENDORSE, 
                        src:ESCROWSETUP::integer AS ESCROWSETUP, 
                        src:MISCSETUP::integer AS MISCSETUP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RCPTFTR::varchar AS RCPTFTR, 
                        src:RCPTHDR::varchar AS RCPTHDR, 
                        src:RCPTOMITTX::varchar AS RCPTOMITTX, 
                        src:REGISTERGROUP::varchar AS REGISTERGROUP, 
                        src:REGISTERGROUPSETUPKEY::integer AS REGISTERGROUPSETUPKEY, 
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
    
                        
                src:REGISTERGROUPSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REGISTERGROUPSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_REGISTERGROUPSETUP as strm))