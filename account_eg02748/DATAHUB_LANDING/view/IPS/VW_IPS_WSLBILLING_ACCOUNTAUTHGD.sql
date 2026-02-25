CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_ACCOUNTAUTHGD AS SELECT
                        src:ACCOUNTAUTHGDKEY::integer AS ACCOUNTAUTHGDKEY, 
                        src:ACCOUNTEXTN::integer AS ACCOUNTEXTN, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTHORISED::varchar AS AUTHORISED, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:GRANTED::varchar AS GRANTED, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RELTOOWNER::varchar AS RELTOOWNER, 
                        src:RELTOPROP::varchar AS RELTOPROP, 
                        src:STARTDATE::datetime AS STARTDATE, 
                        src:STOPDATE::datetime AS STOPDATE, 
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
    
                        
                src:ACCOUNTAUTHGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTAUTHGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_ACCOUNTAUTHGD as strm))