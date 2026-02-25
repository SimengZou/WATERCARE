CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XPROJCOMPINPPRECONDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJINSPDTLKEY::integer AS APPROJINSPDTLKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENGAUDITREQ::varchar AS ENGAUDITREQ, 
                        src:ENGAUDITREQREASON::varchar AS ENGAUDITREQREASON, 
                        src:ENGFINALWOREQ::varchar AS ENGFINALWOREQ, 
                        src:ENGFINALWOREQREASON::varchar AS ENGFINALWOREQREASON, 
                        src:ENGPRECONONSITE::varchar AS ENGPRECONONSITE, 
                        src:ENGPRECONONSITEREASON::varchar AS ENGPRECONONSITEREASON, 
                        src:ENGSHUTDOWNREQ::varchar AS ENGSHUTDOWNREQ, 
                        src:ENGSHUTDOWNREQREASON::varchar AS ENGSHUTDOWNREQREASON, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONSITEMEETACTUALDATE::datetime AS ONSITEMEETACTUALDATE, 
                        src:SENDTOADVISOR::varchar AS SENDTOADVISOR, 
                        src:TECHAUDITREQ::varchar AS TECHAUDITREQ, 
                        src:TECHAUDITREQREASON::varchar AS TECHAUDITREQREASON, 
                        src:TECHFINALWOREQ::varchar AS TECHFINALWOREQ, 
                        src:TECHFINALWOREQREASON::varchar AS TECHFINALWOREQREASON, 
                        src:TECHPRECONONSITE::varchar AS TECHPRECONONSITE, 
                        src:TECHPRECONONSITEREASON::varchar AS TECHPRECONONSITEREASON, 
                        src:TECHSHUTDOWNREQ::varchar AS TECHSHUTDOWNREQ, 
                        src:TECHSHUTDOWNREQREASON::varchar AS TECHSHUTDOWNREQREASON, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTEWATERCONNTYPE::varchar AS WASTEWATERCONNTYPE, 
                        src:XPROJCOMPINPPRECONDPKEY::integer AS XPROJCOMPINPPRECONDPKEY, 
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
    
                        
                src:XPROJCOMPINPPRECONDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPROJCOMPINPPRECONDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XPROJCOMPINPPRECONDP as strm))