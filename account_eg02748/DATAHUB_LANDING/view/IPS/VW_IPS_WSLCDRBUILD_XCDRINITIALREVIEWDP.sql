CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XCDRINITIALREVIEWDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGREVDTLKEY::integer AS APBLDGREVDTLKEY, 
                        src:ARBORIST::varchar AS ARBORIST, 
                        src:BACKFLOWTYPE::varchar AS BACKFLOWTYPE, 
                        src:CRITICALLINES::varchar AS CRITICALLINES, 
                        src:DELETED::boolean AS DELETED, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:MAINLOC::varchar AS MAINLOC, 
                        src:METERBOXSIZE::varchar AS METERBOXSIZE, 
                        src:METERBOXTYPE::varchar AS METERBOXTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PIPEMATERIAL::varchar AS PIPEMATERIAL, 
                        src:SERVICELEAD::varchar AS SERVICELEAD, 
                        src:SIZEMETER::varchar AS SIZEMETER, 
                        src:TMP::varchar AS TMP, 
                        src:VACPITNO::varchar AS VACPITNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTEWATERINSTALLREQUIRED::varchar AS WASTEWATERINSTALLREQUIRED, 
                        src:WASTEWATERINSTALLTYPE::varchar AS WASTEWATERINSTALLTYPE, 
                        src:WATERMAINLOC::varchar AS WATERMAINLOC, 
                        src:WATERMAINLOCATION::varchar AS WATERMAINLOCATION, 
                        src:WATERMAINMATERIAL::varchar AS WATERMAINMATERIAL, 
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, 
                        src:XCDRINITIALREVIEWDPKEY::integer AS XCDRINITIALREVIEWDPKEY, 
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
    
                        
                src:XCDRINITIALREVIEWDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCDRINITIALREVIEWDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XCDRINITIALREVIEWDP as strm))