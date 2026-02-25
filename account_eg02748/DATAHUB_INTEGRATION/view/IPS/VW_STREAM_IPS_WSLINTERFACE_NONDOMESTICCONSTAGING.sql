CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:NONDOMESTICCONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGINSPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NONDOMESTICCONSTAGINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null