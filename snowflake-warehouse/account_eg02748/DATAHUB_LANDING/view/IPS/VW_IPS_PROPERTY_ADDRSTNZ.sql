CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PROPERTY_ADDRSTNZ AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESTNOHI::integer AS ESTNOHI, 
                        src:ESTNOLO::integer AS ESTNOLO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OSTNOHI::integer AS OSTNOHI, 
                        src:OSTNOLO::integer AS OSTNOLO, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:STATE::varchar AS STATE, 
                        src:STNAME::varchar AS STNAME, 
                        src:STNMKEY::integer AS STNMKEY, 
                        src:STTYPE::varchar AS STTYPE, 
                        src:SUBURB::varchar AS SUBURB, 
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
    
                        
                src:STNMKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:STNMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PROPERTY_ADDRSTNZ as strm))