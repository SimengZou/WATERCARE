CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPKEY::integer AS APBLDGINSPKEY, 
                        src:ARBORIST::varchar AS ARBORIST, 
                        src:BACKFLOWREQUIRED::varchar AS BACKFLOWREQUIRED, 
                        src:BACKFLOWSTRAINER::varchar AS BACKFLOWSTRAINER, 
                        src:BACKFLOWTYPE::varchar AS BACKFLOWTYPE, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CRITICALLINES::varchar AS CRITICALLINES, 
                        src:CUSTOMERTMP::varchar AS CUSTOMERTMP, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:INSPECTIONTYPE::varchar AS INSPECTIONTYPE, 
                        src:MAGFLOWPOWERTYPE::varchar AS MAGFLOWPOWERTYPE, 
                        src:MAGFLOWREQUIRED::varchar AS MAGFLOWREQUIRED, 
                        src:METERBOXSIZE::varchar AS METERBOXSIZE, 
                        src:METERBOXTYPE::varchar AS METERBOXTYPE, 
                        src:METERID::varchar AS METERID, 
                        src:METERLOCATION::varchar AS METERLOCATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NONDOMESTICCONSTAGINGKEY::integer AS NONDOMESTICCONSTAGINGKEY, 
                        src:NONDOMMULTIPLESERVICESKEY::integer AS NONDOMMULTIPLESERVICESKEY, 
                        src:PLANSATTACHED::varchar AS PLANSATTACHED, 
                        src:PROPOSEDLOCATION::varchar AS PROPOSEDLOCATION, 
                        src:REMOTEREADER::varchar AS REMOTEREADER, 
                        src:SIZEMETER::varchar AS SIZEMETER, 
                        src:SPRINKLERSUPPLYPIPESIZE::integer AS SPRINKLERSUPPLYPIPESIZE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERMAINLOCATION::varchar AS WATERMAINLOCATION, 
                        src:WATERMAINMATERIAL::varchar AS WATERMAINMATERIAL, 
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:NONDOMMULTIPLESERVICESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGINSPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NONDOMESTICCONSTAGINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NONDOMMULTIPLESERVICESKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null