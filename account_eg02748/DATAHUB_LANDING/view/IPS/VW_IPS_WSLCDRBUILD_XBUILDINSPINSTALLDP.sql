CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDINSPINSTALLDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGINSPDTLKEY::integer AS APBLDGINSPDTLKEY, 
                        src:BACKFLOW::varchar AS BACKFLOW, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISCONNECTION::varchar AS DISCONNECTION, 
                        src:INSTALLCOMPLETE::datetime AS INSTALLCOMPLETE, 
                        src:METERINSTALLATION::varchar AS METERINSTALLATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOWORK::varchar AS NOWORK, 
                        src:REINSTATEMENTCOMPLETE::datetime AS REINSTATEMENTCOMPLETE, 
                        src:RELOCATION::varchar AS RELOCATION, 
                        src:RELOCATIONREPLACEDMETER::varchar AS RELOCATIONREPLACEDMETER, 
                        src:SERVICELINEINSTALLATION::varchar AS SERVICELINEINSTALLATION, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDINSPINSTALLDPKEY::integer AS XBUILDINSPINSTALLDPKEY, 
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
    
                        
                src:XBUILDINSPINSTALLDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDINSPINSTALLDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDINSPINSTALLDP as strm))