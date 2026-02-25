CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XPROJEPARCLODGEDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APLICATIONTYPE::varchar AS APLICATIONTYPE, 
                        src:APPLICATIONREF::varchar AS APPLICATIONREF, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:AREA::varchar AS AREA, 
                        src:DECONTACTNO::varchar AS DECONTACTNO, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEVENG::varchar AS DEVENG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NODOCS::integer AS NODOCS, 
                        src:OTHERTYPE::varchar AS OTHERTYPE, 
                        src:PMCONTACTNO::varchar AS PMCONTACTNO, 
                        src:PROJMAN::varchar AS PROJMAN, 
                        src:PROJREFQPNAME::varchar AS PROJREFQPNAME, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPROJEPARCLODGEDPKEY::integer AS XPROJEPARCLODGEDPKEY, 
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
    
                        
                src:XPROJEPARCLODGEDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPROJEPARCLODGEDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XPROJEPARCLODGEDP as strm))