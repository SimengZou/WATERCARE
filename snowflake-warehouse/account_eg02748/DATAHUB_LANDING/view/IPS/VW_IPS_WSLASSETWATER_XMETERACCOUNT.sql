CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETWATER_XMETERACCOUNT AS SELECT
                        src:"ACCOUNT"::integer AS "ACCOUNT", 
                        src:ACCTSRVPOSKEY::integer AS ACCTSRVPOSKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SERVICETYPE::varchar AS SERVICETYPE, 
                        src:SHAREPERC::numeric(38, 10) AS SHAREPERC, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:SUBTRACTFLAG::varchar AS SUBTRACTFLAG, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XMETERACCOUNTKEY::integer AS XMETERACCOUNTKEY, 
                        src:XWATERMETERDETAILKEY::integer AS XWATERMETERDETAILKEY, 
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
    
                        
                src:XMETERACCOUNTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XMETERACCOUNTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETWATER_XMETERACCOUNT as strm))