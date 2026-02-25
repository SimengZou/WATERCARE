CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCOUNTSERVICE AS SELECT
                        src:AACCALCULATIONDT::datetime AS AACCALCULATIONDT, 
                        src:AACEXCEPTION::integer AS AACEXCEPTION, 
                        src:AACMONTHLYAVERAGE::numeric(38, 10) AS AACMONTHLYAVERAGE, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:AREA::varchar AS AREA, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:IMPERVIOUSPERCENTAGE::numeric(38, 10) AS IMPERVIOUSPERCENTAGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SERVICECLASS1::varchar AS SERVICECLASS1, 
                        src:SERVICECLASS2::varchar AS SERVICECLASS2, 
                        src:SERVICEFIELD1VALUE::numeric(38, 10) AS SERVICEFIELD1VALUE, 
                        src:SERVICEFIELD2VALUE::numeric(38, 10) AS SERVICEFIELD2VALUE, 
                        src:SERVICEFIELD3VALUE::numeric(38, 10) AS SERVICEFIELD3VALUE, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSDTTM::datetime AS STATUSDTTM, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
                        src:SUSPENDFROMDATE::datetime AS SUSPENDFROMDATE, 
                        src:SUSPENDTODATE::datetime AS SUSPENDTODATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WINAVGCALDATE::datetime AS WINAVGCALDATE, 
                        src:WINTERAVG::numeric(38, 10) AS WINTERAVG, 
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
    
                        
                src:ACCOUNTSERVICEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTSERVICEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCOUNTSERVICE as strm))