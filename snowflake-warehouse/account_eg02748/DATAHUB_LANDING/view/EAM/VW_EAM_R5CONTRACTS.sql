CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CONTRACTS AS SELECT
                        src:CON_CLASS::varchar AS CON_CLASS, 
                        src:CON_CLASS_ORG::varchar AS CON_CLASS_ORG, 
                        src:CON_CODE::varchar AS CON_CODE, 
                        src:CON_CONTACT::varchar AS CON_CONTACT, 
                        src:CON_COPY::varchar AS CON_COPY, 
                        src:CON_CREATE::datetime AS CON_CREATE, 
                        src:CON_CURR::varchar AS CON_CURR, 
                        src:CON_DESC::varchar AS CON_DESC, 
                        src:CON_END::datetime AS CON_END, 
                        src:CON_LANG::varchar AS CON_LANG, 
                        src:CON_LASTSAVED::datetime AS CON_LASTSAVED, 
                        src:CON_ORG::varchar AS CON_ORG, 
                        src:CON_OWN::varchar AS CON_OWN, 
                        src:CON_OWNCONTACT::varchar AS CON_OWNCONTACT, 
                        src:CON_PRINTED::varchar AS CON_PRINTED, 
                        src:CON_REF::varchar AS CON_REF, 
                        src:CON_RENEW::datetime AS CON_RENEW, 
                        src:CON_RSTATUS::varchar AS CON_RSTATUS, 
                        src:CON_START::datetime AS CON_START, 
                        src:CON_STATUS::varchar AS CON_STATUS, 
                        src:CON_STORE::varchar AS CON_STORE, 
                        src:CON_SUPPLIER::varchar AS CON_SUPPLIER, 
                        src:CON_SUPPLIER_ORG::varchar AS CON_SUPPLIER_ORG, 
                        src:CON_UPDATECOUNT::numeric(38, 10) AS CON_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:CON_CODE,
            src:CON_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CON_CODE  ORDER BY 
            src:CON_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CONTRACTS as strm))