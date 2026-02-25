CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES AS SELECT
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
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, 
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
    
                        
                src:NONDOMMULTIPLESERVICESKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:NONDOMMULTIPLESERVICESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES as strm))