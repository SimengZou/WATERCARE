CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDINSPMETERDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPDTLKEY::integer AS APBLDGINSPDTLKEY, 
                        src:ARBORIST::varchar AS ARBORIST, 
                        src:BACKFLOWREQUIRED::varchar AS BACKFLOWREQUIRED, 
                        src:BACKFLOWTYPE::varchar AS BACKFLOWTYPE, 
                        src:CRITICALLINES::varchar AS CRITICALLINES, 
                        src:DELETED::boolean AS DELETED, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:ESTIMATEDBYWSL::varchar AS ESTIMATEDBYWSL, 
                        src:ESTTOTAL::numeric(38, 10) AS ESTTOTAL, 
                        src:FIREMETERID::varchar AS FIREMETERID, 
                        src:FIREMETERINSTALLDATE::datetime AS FIREMETERINSTALLDATE, 
                        src:FIREMETERINSTALLEDBY::varchar AS FIREMETERINSTALLEDBY, 
                        src:FIREMETERLOCATION::varchar AS FIREMETERLOCATION, 
                        src:FIREMETERSIZE::varchar AS FIREMETERSIZE, 
                        src:INDUSTRY::varchar AS INDUSTRY, 
                        src:MAGFLOWPOWERTYPE::varchar AS MAGFLOWPOWERTYPE, 
                        src:MAGFLOWREQUIRED::varchar AS MAGFLOWREQUIRED, 
                        src:METERBOXSIZE::varchar AS METERBOXSIZE, 
                        src:METERBOXTYPE::varchar AS METERBOXTYPE, 
                        src:METERID::varchar AS METERID, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PEAKFLOW::numeric(38, 10) AS PEAKFLOW, 
                        src:PIPEMATERIAL::varchar AS PIPEMATERIAL, 
                        src:PRICINGPLAN::varchar AS PRICINGPLAN, 
                        src:REMOTEREADER::varchar AS REMOTEREADER, 
                        src:SIZEMETER::varchar AS SIZEMETER, 
                        src:SPRINKLERSUPPLYPIPESIZE::integer AS SPRINKLERSUPPLYPIPESIZE, 
                        src:STRAINER::varchar AS STRAINER, 
                        src:TMP::varchar AS TMP, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERMAINLOC::varchar AS WATERMAINLOC, 
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, 
                        src:WATERNEWMETERSIZE::varchar AS WATERNEWMETERSIZE, 
                        src:WORKTYPE::varchar AS WORKTYPE, 
                        src:XBUILDINSPMETERDPKEY::integer AS XBUILDINSPMETERDPKEY, 
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
    
                        
                src:XBUILDINSPMETERDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDINSPMETERDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDINSPMETERDP as strm))