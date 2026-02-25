CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCOUNTTYPESETUP AS SELECT
                        src:ACCOUNTTYPESETUP::varchar AS ACCOUNTTYPESETUP, 
                        src:ACCOUNTTYPESETUPKEY::integer AS ACCOUNTTYPESETUPKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSNUMBERFORMAT::integer AS ADDRESSNUMBERFORMAT, 
                        src:APPLICATIONNUMBERFORMAT::integer AS APPLICATIONNUMBERFORMAT, 
                        src:CATEGORY::integer AS CATEGORY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DETAILPAGE::integer AS DETAILPAGE, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:HASACCOUNTSERVICES::varchar AS HASACCOUNTSERVICES, 
                        src:HASADDRESSSERVICES::varchar AS HASADDRESSSERVICES, 
                        src:HASCDRSERVICES::varchar AS HASCDRSERVICES, 
                        src:HASSUNDRYSERVICES::varchar AS HASSUNDRYSERVICES, 
                        src:IDENTITYNUMBERFORMAT::integer AS IDENTITYNUMBERFORMAT, 
                        src:ISSUSPENSEACCTTYPEFLAG::varchar AS ISSUSPENSEACCTTYPEFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OCCUPANCY::varchar AS OCCUPANCY, 
                        src:PARCELNUMBERFORMAT::integer AS PARCELNUMBERFORMAT, 
                        src:PROPERTYNUMBERFORMAT::integer AS PROPERTYNUMBERFORMAT, 
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
    
                        
                src:ACCOUNTTYPESETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTTYPESETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCOUNTTYPESETUP as strm))