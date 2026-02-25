CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCUSTOMER_XSMSDETAILS AS SELECT
                        src:ACTIVITYKEY::integer AS ACTIVITYKEY, 
                        src:ACTIVITYTYPE::varchar AS ACTIVITYTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MESSAGEID::varchar AS MESSAGEID, 
                        src:MESSAGERECEIVED::varchar AS MESSAGERECEIVED, 
                        src:MOBILENUMBER::varchar AS MOBILENUMBER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RESPONSEREQ::varchar AS RESPONSEREQ, 
                        src:STATUS::varchar AS STATUS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XSMSDETAILSKEY::integer AS XSMSDETAILSKEY, 
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
    
                        
                src:XSMSDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XSMSDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCUSTOMER_XSMSDETAILS as strm))