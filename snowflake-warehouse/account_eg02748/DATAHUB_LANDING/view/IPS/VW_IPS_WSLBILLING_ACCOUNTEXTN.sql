CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_ACCOUNTEXTN AS SELECT
                        src:ACCOUNTEXTNKEY::integer AS ACCOUNTEXTNKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACENTITY::varchar AS ACENTITY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELIVERYMETHOD::varchar AS DELIVERYMETHOD, 
                        src:HANSENAUTHORITY::varchar AS HANSENAUTHORITY, 
                        src:IGCAPPLICATION::varchar AS IGCAPPLICATION, 
                        src:IGCBASELINE::numeric(38, 10) AS IGCBASELINE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OWNEREMAIL::varchar AS OWNEREMAIL, 
                        src:PIVOTAL::varchar AS PIVOTAL, 
                        src:PMEMAIL::varchar AS PMEMAIL, 
                        src:REMOVEHANSENAUTH::varchar AS REMOVEHANSENAUTH, 
                        src:STARTDATE::datetime AS STARTDATE, 
                        src:STOPDATE::datetime AS STOPDATE, 
                        src:TENANTEMAIL::varchar AS TENANTEMAIL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WEBEMAILCC::varchar AS WEBEMAILCC, 
                        src:WEBEMAILTO::varchar AS WEBEMAILTO, 
                        src:XERO::varchar AS XERO, 
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
    
                        
                src:ACCOUNTEXTNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTEXTNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_ACCOUNTEXTN as strm))