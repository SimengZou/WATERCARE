CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XCDRINITIALREVIEWDP AS SELECT
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
                        src:XCDRINITIALREVIEWDPKEY::integer AS XCDRINITIALREVIEWDPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XCDRINITIALREVIEWDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XCDRINITIALREVIEWDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGREVDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCDRINITIALREVIEWDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null