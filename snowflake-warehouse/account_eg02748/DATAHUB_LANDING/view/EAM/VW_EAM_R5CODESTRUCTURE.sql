CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CODESTRUCTURE AS SELECT
                        src:CSR_CODE::varchar AS CSR_CODE, 
                        src:CSR_DESC::varchar AS CSR_DESC, 
                        src:CSR_ENTITY::varchar AS CSR_ENTITY, 
                        src:CSR_ICON::varchar AS CSR_ICON, 
                        src:CSR_ICONPATH::varchar AS CSR_ICONPATH, 
                        src:CSR_IMAGE::varchar AS CSR_IMAGE, 
                        src:CSR_IMPORTANCE::varchar AS CSR_IMPORTANCE, 
                        src:CSR_LASTSAVED::datetime AS CSR_LASTSAVED, 
                        src:CSR_MATERIALTYPE::varchar AS CSR_MATERIALTYPE, 
                        src:CSR_NOTUSED::varchar AS CSR_NOTUSED, 
                        src:CSR_RENTITY::varchar AS CSR_RENTITY, 
                        src:CSR_STRLEVEL1::varchar AS CSR_STRLEVEL1, 
                        src:CSR_STRLEVEL2::varchar AS CSR_STRLEVEL2, 
                        src:CSR_STRLEVEL3::varchar AS CSR_STRLEVEL3, 
                        src:CSR_STRLEVEL4::varchar AS CSR_STRLEVEL4, 
                        src:CSR_STRLEVEL5::varchar AS CSR_STRLEVEL5, 
                        src:CSR_STRLEVEL6::varchar AS CSR_STRLEVEL6, 
                        src:CSR_STRLEVEL7::varchar AS CSR_STRLEVEL7, 
                        src:CSR_STRLEVEL8::varchar AS CSR_STRLEVEL8, 
                        src:CSR_STRUCTURE::varchar AS CSR_STRUCTURE, 
                        src:CSR_UPDATECOUNT::numeric(38, 10) AS CSR_UPDATECOUNT, 
                        src:CSR_UPDATED::datetime AS CSR_UPDATED, 
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
    
                        
                src:CSR_CODE,
            src:CSR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CSR_CODE  ORDER BY 
            src:CSR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CODESTRUCTURE as strm))