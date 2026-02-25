CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XPROJCOMPRVWPRECONDP AS SELECT
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
                        src:XPROJCOMPRVWPRECONDPKEY::integer AS XPROJCOMPRVWPRECONDPKEY, 
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
    
                        
                src:XPROJCOMPRVWPRECONDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPROJCOMPRVWPRECONDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XPROJCOMPRVWPRECONDP as strm))