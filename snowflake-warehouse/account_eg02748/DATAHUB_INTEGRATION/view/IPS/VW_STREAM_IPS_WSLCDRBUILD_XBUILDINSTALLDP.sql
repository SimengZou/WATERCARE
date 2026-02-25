CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSTALLDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGAPPLDTLKEY::integer AS APBLDGAPPLDTLKEY, 
                        src:BACKFLOW::varchar AS BACKFLOW, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISCONNECTION::varchar AS DISCONNECTION, 
                        src:INSTALLCOMPLETE::datetime AS INSTALLCOMPLETE, 
                        src:METERINSTALLATION::varchar AS METERINSTALLATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REINSTATEMENTCOMPLETE::datetime AS REINSTATEMENTCOMPLETE, 
                        src:RELOCATION::varchar AS RELOCATION, 
                        src:RELOCATIONREPLACEDMETER::varchar AS RELOCATIONREPLACEDMETER, 
                        src:SERVICELINEINSTALLATION::varchar AS SERVICELINEINSTALLATION, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDINSTALLDPKEY::integer AS XBUILDINSTALLDPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XBUILDINSTALLDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDINSTALLDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGAPPLDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTALLCOMPLETE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REINSTATEMENTCOMPLETE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XBUILDINSTALLDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null