CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDAPPCR AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOCATED::varchar AS ALLOCATED, 
                        src:APBLDGKEY::integer AS APBLDGKEY, 
                        src:APTYPE::varchar AS APTYPE, 
                        src:CONTRACTOR::varchar AS CONTRACTOR, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEVELOPERID::varchar AS DEVELOPERID, 
                        src:DEVELOPMENTAREA::varchar AS DEVELOPMENTAREA, 
                        src:FLOORAREA::varchar AS FLOORAREA, 
                        src:GISLATERAL::varchar AS GISLATERAL, 
                        src:IGCZONECODE::varchar AS IGCZONECODE, 
                        src:MASTERAPNO::varchar AS MASTERAPNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OWNERROLE::varchar AS OWNERROLE, 
                        src:PARENTAPPLICATIONKEY::integer AS PARENTAPPLICATIONKEY, 
                        src:PARENTAPTYPE::varchar AS PARENTAPTYPE, 
                        src:RELOCATIONTYPE::varchar AS RELOCATIONTYPE, 
                        src:RESUBMITWORKREQUEST::varchar AS RESUBMITWORKREQUEST, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDAPPCRKEY::integer AS XBUILDAPPCRKEY, 
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
    
                        
                src:XBUILDAPPCRKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDAPPCRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDAPPCR as strm))