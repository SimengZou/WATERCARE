CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_CONNECTIONSSTAGING AS SELECT
                        src:ACTIONTYPE::varchar AS ACTIONTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPKEY::integer AS APBLDGINSPKEY, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:APNO::varchar AS APNO, 
                        src:APPLICATIONTYPE::varchar AS APPLICATIONTYPE, 
                        src:ARBORIST::varchar AS ARBORIST, 
                        src:BACKFLOWREQUIRED::varchar AS BACKFLOWREQUIRED, 
                        src:BACKFLOWSTRAINER::varchar AS BACKFLOWSTRAINER, 
                        src:BACKFLOWTYPE::varchar AS BACKFLOWTYPE, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONNECTIONSSTAGINGKEY::integer AS CONNECTIONSSTAGINGKEY, 
                        src:CONTRACTOR::varchar AS CONTRACTOR, 
                        src:CRITICALLINES::varchar AS CRITICALLINES, 
                        src:CUSTOMERTMP::varchar AS CUSTOMERTMP, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DP::varchar AS DP, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:ENGINEERFULLNAME::varchar AS ENGINEERFULLNAME, 
                        src:FLAT::varchar AS FLAT, 
                        src:GISLATERAL::varchar AS GISLATERAL, 
                        src:HOUSENUMBER::varchar AS HOUSENUMBER, 
                        src:LEGALOWNEREMAIL::varchar AS LEGALOWNEREMAIL, 
                        src:LEGALOWNERFULLNAME::varchar AS LEGALOWNERFULLNAME, 
                        src:LEGALOWNERMOBILE::varchar AS LEGALOWNERMOBILE, 
                        src:LOT::varchar AS LOT, 
                        src:METERBOXTYPE::varchar AS METERBOXTYPE, 
                        src:METERID::varchar AS METERID, 
                        src:METERLOCATION::varchar AS METERLOCATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OCCUPANCYTYPE::varchar AS OCCUPANCYTYPE, 
                        src:ONSITEEMAIL::varchar AS ONSITEEMAIL, 
                        src:ONSITEFULLNAME::varchar AS ONSITEFULLNAME, 
                        src:ONSITEMOBILE::varchar AS ONSITEMOBILE, 
                        src:PLANSATTACHED::varchar AS PLANSATTACHED, 
                        src:POSTALCODE::varchar AS POSTALCODE, 
                        src:PROPOSEDLOCATION::varchar AS PROPOSEDLOCATION, 
                        src:RELOCATIONTYPE::varchar AS RELOCATIONTYPE, 
                        src:REQUESTTYPE::varchar AS REQUESTTYPE, 
                        src:SPRINKLERSUPPLYPIPESIZE::integer AS SPRINKLERSUPPLYPIPESIZE, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STREETTYPE::varchar AS STREETTYPE, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERMAINLOCATION::varchar AS WATERMAINLOCATION, 
                        src:WATERMAINMATERIAL::varchar AS WATERMAINMATERIAL, 
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, 
                        src:WATERMETERSIZE::varchar AS WATERMETERSIZE, 
                        src:WORKTYPE::varchar AS WORKTYPE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:CONNECTIONSSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_CONNECTIONSSTAGING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGINSPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONNECTIONSSTAGINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null