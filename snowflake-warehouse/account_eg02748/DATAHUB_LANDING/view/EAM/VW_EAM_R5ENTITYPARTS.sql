CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ENTITYPARTS AS SELECT
                        src:EPA_ASMLEVEL::varchar AS EPA_ASMLEVEL, 
                        src:EPA_CODE::varchar AS EPA_CODE, 
                        src:EPA_COMMENT::varchar AS EPA_COMMENT, 
                        src:EPA_COMPLEVEL::varchar AS EPA_COMPLEVEL, 
                        src:EPA_ENTITY::varchar AS EPA_ENTITY, 
                        src:EPA_LASTSAVED::datetime AS EPA_LASTSAVED, 
                        src:EPA_PART::varchar AS EPA_PART, 
                        src:EPA_PARTLOCATION::varchar AS EPA_PARTLOCATION, 
                        src:EPA_PART_ORG::varchar AS EPA_PART_ORG, 
                        src:EPA_PK::varchar AS EPA_PK, 
                        src:EPA_QTY::numeric(38, 10) AS EPA_QTY, 
                        src:EPA_RENTITY::varchar AS EPA_RENTITY, 
                        src:EPA_RTYPE::varchar AS EPA_RTYPE, 
                        src:EPA_SYSLEVEL::varchar AS EPA_SYSLEVEL, 
                        src:EPA_TYPE::varchar AS EPA_TYPE, 
                        src:EPA_UOM::varchar AS EPA_UOM, 
                        src:EPA_UPDATECOUNT::numeric(38, 10) AS EPA_UPDATECOUNT, 
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
    
                        
                src:EPA_PK,
            src:EPA_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EPA_PK  ORDER BY 
            src:EPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ENTITYPARTS as strm))