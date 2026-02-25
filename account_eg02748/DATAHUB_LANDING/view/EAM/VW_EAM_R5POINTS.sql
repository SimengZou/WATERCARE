CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5POINTS AS SELECT
                        src:POI_CREATED::datetime AS POI_CREATED, 
                        src:POI_DESC::varchar AS POI_DESC, 
                        src:POI_LASTSAVED::datetime AS POI_LASTSAVED, 
                        src:POI_OBJECT::varchar AS POI_OBJECT, 
                        src:POI_OBJECT_ORG::varchar AS POI_OBJECT_ORG, 
                        src:POI_OBRTYPE::varchar AS POI_OBRTYPE, 
                        src:POI_OBTYPE::varchar AS POI_OBTYPE, 
                        src:POI_POINT::varchar AS POI_POINT, 
                        src:POI_POINTTYPE::varchar AS POI_POINTTYPE, 
                        src:POI_UDFCHAR01::varchar AS POI_UDFCHAR01, 
                        src:POI_UDFCHAR02::varchar AS POI_UDFCHAR02, 
                        src:POI_UDFCHAR03::varchar AS POI_UDFCHAR03, 
                        src:POI_UDFCHAR04::varchar AS POI_UDFCHAR04, 
                        src:POI_UDFCHAR05::varchar AS POI_UDFCHAR05, 
                        src:POI_UDFCHAR06::varchar AS POI_UDFCHAR06, 
                        src:POI_UDFCHAR07::varchar AS POI_UDFCHAR07, 
                        src:POI_UDFCHAR08::varchar AS POI_UDFCHAR08, 
                        src:POI_UDFCHAR09::varchar AS POI_UDFCHAR09, 
                        src:POI_UDFCHAR10::varchar AS POI_UDFCHAR10, 
                        src:POI_UDFCHAR11::varchar AS POI_UDFCHAR11, 
                        src:POI_UDFCHAR12::varchar AS POI_UDFCHAR12, 
                        src:POI_UDFCHAR13::varchar AS POI_UDFCHAR13, 
                        src:POI_UDFCHAR14::varchar AS POI_UDFCHAR14, 
                        src:POI_UDFCHAR15::varchar AS POI_UDFCHAR15, 
                        src:POI_UDFCHAR16::varchar AS POI_UDFCHAR16, 
                        src:POI_UDFCHAR17::varchar AS POI_UDFCHAR17, 
                        src:POI_UDFCHAR18::varchar AS POI_UDFCHAR18, 
                        src:POI_UDFCHAR19::varchar AS POI_UDFCHAR19, 
                        src:POI_UDFCHAR20::varchar AS POI_UDFCHAR20, 
                        src:POI_UDFCHAR21::varchar AS POI_UDFCHAR21, 
                        src:POI_UDFCHAR22::varchar AS POI_UDFCHAR22, 
                        src:POI_UDFCHAR23::varchar AS POI_UDFCHAR23, 
                        src:POI_UDFCHAR24::varchar AS POI_UDFCHAR24, 
                        src:POI_UDFCHAR25::varchar AS POI_UDFCHAR25, 
                        src:POI_UDFCHAR26::varchar AS POI_UDFCHAR26, 
                        src:POI_UDFCHAR27::varchar AS POI_UDFCHAR27, 
                        src:POI_UDFCHAR28::varchar AS POI_UDFCHAR28, 
                        src:POI_UDFCHAR29::varchar AS POI_UDFCHAR29, 
                        src:POI_UDFCHAR30::varchar AS POI_UDFCHAR30, 
                        src:POI_UDFCHKBOX01::varchar AS POI_UDFCHKBOX01, 
                        src:POI_UDFCHKBOX02::varchar AS POI_UDFCHKBOX02, 
                        src:POI_UDFCHKBOX03::varchar AS POI_UDFCHKBOX03, 
                        src:POI_UDFCHKBOX04::varchar AS POI_UDFCHKBOX04, 
                        src:POI_UDFCHKBOX05::varchar AS POI_UDFCHKBOX05, 
                        src:POI_UDFDATE01::datetime AS POI_UDFDATE01, 
                        src:POI_UDFDATE02::datetime AS POI_UDFDATE02, 
                        src:POI_UDFDATE03::datetime AS POI_UDFDATE03, 
                        src:POI_UDFDATE04::datetime AS POI_UDFDATE04, 
                        src:POI_UDFDATE05::datetime AS POI_UDFDATE05, 
                        src:POI_UDFNUM01::numeric(38, 10) AS POI_UDFNUM01, 
                        src:POI_UDFNUM02::numeric(38, 10) AS POI_UDFNUM02, 
                        src:POI_UDFNUM03::numeric(38, 10) AS POI_UDFNUM03, 
                        src:POI_UDFNUM04::numeric(38, 10) AS POI_UDFNUM04, 
                        src:POI_UDFNUM05::numeric(38, 10) AS POI_UDFNUM05, 
                        src:POI_UPDATECOUNT::numeric(38, 10) AS POI_UPDATECOUNT, 
                        src:POI_UPDATED::datetime AS POI_UPDATED, 
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
    
                        
                src:POI_OBJECT,
                src:POI_OBJECT_ORG,
                src:POI_OBTYPE,
                src:POI_POINT,
                src:POI_POINTTYPE,
            src:POI_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:POI_OBJECT,
                src:POI_OBJECT_ORG,
                src:POI_OBTYPE,
                src:POI_POINT,
                src:POI_POINTTYPE  ORDER BY 
            src:POI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5POINTS as strm))