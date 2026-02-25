CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FINDPROFILE AS SELECT
                        src:FPF_CODE::varchar AS FPF_CODE, 
                        src:FPF_LASTSAVED::datetime AS FPF_LASTSAVED, 
                        src:FPF_PROFILE::varchar AS FPF_PROFILE, 
                        src:FPF_PROFILE_ORG::varchar AS FPF_PROFILE_ORG, 
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
    
                        
                src:FPF_CODE,
                src:FPF_PROFILE_ORG,
            src:FPF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FPF_CODE,
                src:FPF_PROFILE_ORG  ORDER BY 
            src:FPF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FINDPROFILE as strm))