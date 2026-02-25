CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5BINSTOCK AS SELECT
                        src:BIS_BIN::varchar AS BIS_BIN, 
                        src:BIS_CREATED::datetime AS BIS_CREATED, 
                        src:BIS_CREATEDBY::varchar AS BIS_CREATEDBY, 
                        src:BIS_LASTSAVED::datetime AS BIS_LASTSAVED, 
                        src:BIS_LOT::varchar AS BIS_LOT, 
                        src:BIS_PART::varchar AS BIS_PART, 
                        src:BIS_PART_ORG::varchar AS BIS_PART_ORG, 
                        src:BIS_QTY::numeric(38, 10) AS BIS_QTY, 
                        src:BIS_REPAIRQTY::numeric(38, 10) AS BIS_REPAIRQTY, 
                        src:BIS_STORE::varchar AS BIS_STORE, 
                        src:BIS_UDFCHAR01::varchar AS BIS_UDFCHAR01, 
                        src:BIS_UDFCHAR02::varchar AS BIS_UDFCHAR02, 
                        src:BIS_UDFCHAR03::varchar AS BIS_UDFCHAR03, 
                        src:BIS_UDFCHAR04::varchar AS BIS_UDFCHAR04, 
                        src:BIS_UDFCHAR05::varchar AS BIS_UDFCHAR05, 
                        src:BIS_UDFCHAR06::varchar AS BIS_UDFCHAR06, 
                        src:BIS_UDFCHAR07::varchar AS BIS_UDFCHAR07, 
                        src:BIS_UDFCHAR08::varchar AS BIS_UDFCHAR08, 
                        src:BIS_UDFCHAR09::varchar AS BIS_UDFCHAR09, 
                        src:BIS_UDFCHAR10::varchar AS BIS_UDFCHAR10, 
                        src:BIS_UDFCHAR11::varchar AS BIS_UDFCHAR11, 
                        src:BIS_UDFCHAR12::varchar AS BIS_UDFCHAR12, 
                        src:BIS_UDFCHAR13::varchar AS BIS_UDFCHAR13, 
                        src:BIS_UDFCHAR14::varchar AS BIS_UDFCHAR14, 
                        src:BIS_UDFCHAR15::varchar AS BIS_UDFCHAR15, 
                        src:BIS_UDFCHAR16::varchar AS BIS_UDFCHAR16, 
                        src:BIS_UDFCHAR17::varchar AS BIS_UDFCHAR17, 
                        src:BIS_UDFCHAR18::varchar AS BIS_UDFCHAR18, 
                        src:BIS_UDFCHAR19::varchar AS BIS_UDFCHAR19, 
                        src:BIS_UDFCHAR20::varchar AS BIS_UDFCHAR20, 
                        src:BIS_UDFCHAR21::varchar AS BIS_UDFCHAR21, 
                        src:BIS_UDFCHAR22::varchar AS BIS_UDFCHAR22, 
                        src:BIS_UDFCHAR23::varchar AS BIS_UDFCHAR23, 
                        src:BIS_UDFCHAR24::varchar AS BIS_UDFCHAR24, 
                        src:BIS_UDFCHAR25::varchar AS BIS_UDFCHAR25, 
                        src:BIS_UDFCHAR26::varchar AS BIS_UDFCHAR26, 
                        src:BIS_UDFCHAR27::varchar AS BIS_UDFCHAR27, 
                        src:BIS_UDFCHAR28::varchar AS BIS_UDFCHAR28, 
                        src:BIS_UDFCHAR29::varchar AS BIS_UDFCHAR29, 
                        src:BIS_UDFCHAR30::varchar AS BIS_UDFCHAR30, 
                        src:BIS_UDFCHKBOX01::varchar AS BIS_UDFCHKBOX01, 
                        src:BIS_UDFCHKBOX02::varchar AS BIS_UDFCHKBOX02, 
                        src:BIS_UDFCHKBOX03::varchar AS BIS_UDFCHKBOX03, 
                        src:BIS_UDFCHKBOX04::varchar AS BIS_UDFCHKBOX04, 
                        src:BIS_UDFCHKBOX05::varchar AS BIS_UDFCHKBOX05, 
                        src:BIS_UDFDATE01::datetime AS BIS_UDFDATE01, 
                        src:BIS_UDFDATE02::datetime AS BIS_UDFDATE02, 
                        src:BIS_UDFDATE03::datetime AS BIS_UDFDATE03, 
                        src:BIS_UDFDATE04::datetime AS BIS_UDFDATE04, 
                        src:BIS_UDFDATE05::datetime AS BIS_UDFDATE05, 
                        src:BIS_UDFNUM01::numeric(38, 10) AS BIS_UDFNUM01, 
                        src:BIS_UDFNUM02::numeric(38, 10) AS BIS_UDFNUM02, 
                        src:BIS_UDFNUM03::numeric(38, 10) AS BIS_UDFNUM03, 
                        src:BIS_UDFNUM04::numeric(38, 10) AS BIS_UDFNUM04, 
                        src:BIS_UDFNUM05::numeric(38, 10) AS BIS_UDFNUM05, 
                        src:BIS_UPDATECOUNT::numeric(38, 10) AS BIS_UPDATECOUNT, 
                        src:BIS_UPDATED::datetime AS BIS_UPDATED, 
                        src:BIS_UPDATEDBY::varchar AS BIS_UPDATEDBY, 
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
    
                        
                src:BIS_BIN,
                src:BIS_LOT,
                src:BIS_PART,
                src:BIS_PART_ORG,
                src:BIS_STORE,
            src:BIS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BIS_BIN,
                src:BIS_LOT,
                src:BIS_PART,
                src:BIS_PART_ORG,
                src:BIS_STORE  ORDER BY 
            src:BIS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5BINSTOCK as strm))