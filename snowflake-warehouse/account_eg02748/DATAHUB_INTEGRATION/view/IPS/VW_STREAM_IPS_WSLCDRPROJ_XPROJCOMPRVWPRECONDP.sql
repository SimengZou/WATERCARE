CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_XPROJCOMPRVWPRECONDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJREVDTLKEY::integer AS APPROJREVDTLKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENGAUDITREQ::varchar AS ENGAUDITREQ, 
                        src:ENGAUDITREQREASON::varchar AS ENGAUDITREQREASON, 
                        src:ENGEPAASSESS::varchar AS ENGEPAASSESS, 
                        src:ENGFINALWALKOVER::varchar AS ENGFINALWALKOVER, 
                        src:ENGFINALWALKOVERREASON::varchar AS ENGFINALWALKOVERREASON, 
                        src:ENGPRECONONSITE::varchar AS ENGPRECONONSITE, 
                        src:ENGPRECONONSITEREASON::varchar AS ENGPRECONONSITEREASON, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONSITEMEETACTUALDATE::datetime AS ONSITEMEETACTUALDATE, 
                        src:ONSITEMEETSCHEDDATE::datetime AS ONSITEMEETSCHEDDATE, 
                        src:PASSTOADVISOR::varchar AS PASSTOADVISOR, 
                        src:PASSTOTECHNICIAN::varchar AS PASSTOTECHNICIAN, 
                        src:TECHAUDITREQ::varchar AS TECHAUDITREQ, 
                        src:TECHAUDITREQREASON::varchar AS TECHAUDITREQREASON, 
                        src:TECHFINALWALKOVER::varchar AS TECHFINALWALKOVER, 
                        src:TECHFINALWALKOVERREASON::varchar AS TECHFINALWALKOVERREASON, 
                        src:TECHPRECONONSITE::varchar AS TECHPRECONONSITE, 
                        src:TECHPRECONONSITEREASON::varchar AS TECHPRECONONSITEREASON, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTEWATERCONNECTIONTYPE::varchar AS WASTEWATERCONNECTIONTYPE, 
                        src:XPROJCOMPRVWPRECONDPKEY::integer AS XPROJCOMPRVWPRECONDPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XPROJCOMPRVWPRECONDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPROJ_XPROJCOMPRVWPRECONDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPROJREVDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ONSITEMEETACTUALDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ONSITEMEETSCHEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPROJCOMPRVWPRECONDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null