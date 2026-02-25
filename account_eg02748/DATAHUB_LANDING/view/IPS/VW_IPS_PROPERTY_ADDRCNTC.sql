CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PROPERTY_ADDRCNTC AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRCNTCKEY::integer AS ADDRCNTCKEY, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:CONTACTCAPACITY::varchar AS CONTACTCAPACITY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OCCFROMDT::datetime AS OCCFROMDT, 
                        src:OCCTODT::datetime AS OCCTODT, 
                        src:OCCUPANT::varchar AS OCCUPANT, 
                        src:OWNER::varchar AS OWNER, 
                        src:OWNFROMDT::datetime AS OWNFROMDT, 
                        src:OWNTODT::datetime AS OWNTODT, 
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
    
                        
                src:ADDRCNTCKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADDRCNTCKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PROPERTY_ADDRCNTC as strm))