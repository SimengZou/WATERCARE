CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SHIFTS AS SELECT
                        src:SHF_CLASS::varchar AS SHF_CLASS, 
                        src:SHF_CLASS_ORG::varchar AS SHF_CLASS_ORG, 
                        src:SHF_CODE::varchar AS SHF_CODE, 
                        src:SHF_DESC::varchar AS SHF_DESC, 
                        src:SHF_LASTSAVED::datetime AS SHF_LASTSAVED, 
                        src:SHF_LENGTH::numeric(38, 10) AS SHF_LENGTH, 
                        src:SHF_ORG::varchar AS SHF_ORG, 
                        src:SHF_START::datetime AS SHF_START, 
                        src:SHF_UPDATECOUNT::numeric(38, 10) AS SHF_UPDATECOUNT, 
                        src:SHF_UPDATED::datetime AS SHF_UPDATED, 
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
    
                        
                src:SHF_CODE,
            src:SHF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SHF_CODE  ORDER BY 
            src:SHF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SHIFTS as strm))