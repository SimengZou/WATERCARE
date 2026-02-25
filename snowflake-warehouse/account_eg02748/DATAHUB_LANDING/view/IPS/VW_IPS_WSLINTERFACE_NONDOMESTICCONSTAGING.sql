CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING AS SELECT
                        src:ACTIONTYPE::varchar AS ACTIONTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPKEY::integer AS APBLDGINSPKEY, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:APNO::varchar AS APNO, 
                        src:APPLICATIONTYPE::varchar AS APPLICATIONTYPE, 
                        src:CONTRACTOR::varchar AS CONTRACTOR, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DP::varchar AS DP, 
                        src:ENGINEERFULLNAME::varchar AS ENGINEERFULLNAME, 
                        src:FLAT::varchar AS FLAT, 
                        src:HOUSENUMBER::varchar AS HOUSENUMBER, 
                        src:LEGALOWNEREMAIL::varchar AS LEGALOWNEREMAIL, 
                        src:LEGALOWNERFULLNAME::varchar AS LEGALOWNERFULLNAME, 
                        src:LEGALOWNERMOBILE::varchar AS LEGALOWNERMOBILE, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NONDOMESTICCONSTAGINGKEY::integer AS NONDOMESTICCONSTAGINGKEY, 
                        src:ONSITEEMAIL::varchar AS ONSITEEMAIL, 
                        src:ONSITEFULLNAME::varchar AS ONSITEFULLNAME, 
                        src:ONSITEMOBILE::varchar AS ONSITEMOBILE, 
                        src:POSTALCODE::varchar AS POSTALCODE, 
                        src:REQUESTTYPE::varchar AS REQUESTTYPE, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STREETTYPE::varchar AS STREETTYPE, 
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
    
                        
                src:NONDOMESTICCONSTAGINGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:NONDOMESTICCONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLINTERFACE_NONDOMESTICCONSTAGING as strm))