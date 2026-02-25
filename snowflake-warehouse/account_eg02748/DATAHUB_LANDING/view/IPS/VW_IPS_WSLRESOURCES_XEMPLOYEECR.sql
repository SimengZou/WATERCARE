CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLRESOURCES_XEMPLOYEECR AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CAPACITY::integer AS CAPACITY, 
                        src:CAPACITYRATE::numeric(38, 10) AS CAPACITYRATE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EMPID::varchar AS EMPID, 
                        src:EMPLOYEEROLE::varchar AS EMPLOYEEROLE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIDOMCAPACITY::integer AS MULTIDOMCAPACITY, 
                        src:ONLEAVE::varchar AS ONLEAVE, 
                        src:REGION::varchar AS REGION, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XEMPLOYEECRKEY::integer AS XEMPLOYEECRKEY, 
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
    
                        
                src:XEMPLOYEECRKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XEMPLOYEECRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLRESOURCES_XEMPLOYEECR as strm))