CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_U5ASSETSUBTYPES AS SELECT
                        src:AST_ASSETSUBTYPE::varchar AS AST_ASSETSUBTYPE, 
                        src:AST_ASSETTYPE::varchar AS AST_ASSETTYPE, 
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
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
    
                        
                src:AST_ASSETSUBTYPE,
                src:AST_ASSETTYPE,
            src:LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AST_ASSETSUBTYPE,
                src:AST_ASSETTYPE  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_U5ASSETSUBTYPES as strm))