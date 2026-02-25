CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_ACCOUNTSERVICEDELETE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEDELETEKEY::integer AS ACCOUNTSERVICEDELETEKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:AREA::varchar AS AREA, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGADDBY::varchar AS ORIGADDBY, 
                        src:ORIGADDDTTM::datetime AS ORIGADDDTTM, 
                        src:ORIGMODBY::varchar AS ORIGMODBY, 
                        src:ORIGMODDTTM::datetime AS ORIGMODDTTM, 
                        src:SERVICECLASS1::varchar AS SERVICECLASS1, 
                        src:SERVICEFIELD1VALUE::numeric(38, 10) AS SERVICEFIELD1VALUE, 
                        src:SERVICEFIELD2VALUE::numeric(38, 10) AS SERVICEFIELD2VALUE, 
                        src:SERVICEFIELD3VALUE::numeric(38, 10) AS SERVICEFIELD3VALUE, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSDTTM::datetime AS STATUSDTTM, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
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
    
                        
                src:ACCOUNTSERVICEDELETEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTSERVICEDELETEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_ACCOUNTSERVICEDELETE as strm))