CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDAPPCR AS SELECT
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
                        src:XBUILDAPPCRKEY::integer AS XBUILDAPPCRKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XBUILDAPPCRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDAPPCR as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTAPPLICATIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XBUILDAPPCRKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null