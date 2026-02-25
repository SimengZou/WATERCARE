CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5COSTCODES AS SELECT
                        src:CST_CLASS::varchar AS CST_CLASS, 
                        src:CST_CLASS_ORG::varchar AS CST_CLASS_ORG, 
                        src:CST_CODE::varchar AS CST_CODE, 
                        src:CST_CREATED::datetime AS CST_CREATED, 
                        src:CST_CREATEDBY::varchar AS CST_CREATEDBY, 
                        src:CST_DESC::varchar AS CST_DESC, 
                        src:CST_FLEETCUSTOMER::varchar AS CST_FLEETCUSTOMER, 
                        src:CST_FLEETCUSTOMER_ORG::varchar AS CST_FLEETCUSTOMER_ORG, 
                        src:CST_LASTSAVED::datetime AS CST_LASTSAVED, 
                        src:CST_NONBILLABLE::varchar AS CST_NONBILLABLE, 
                        src:CST_NOTUSED::varchar AS CST_NOTUSED, 
                        src:CST_ORG::varchar AS CST_ORG, 
                        src:CST_SEGMENTVALUE::varchar AS CST_SEGMENTVALUE, 
                        src:CST_UDFCHAR01::varchar AS CST_UDFCHAR01, 
                        src:CST_UDFCHAR02::varchar AS CST_UDFCHAR02, 
                        src:CST_UDFCHAR03::varchar AS CST_UDFCHAR03, 
                        src:CST_UDFCHAR04::varchar AS CST_UDFCHAR04, 
                        src:CST_UDFCHAR05::varchar AS CST_UDFCHAR05, 
                        src:CST_UDFCHAR06::varchar AS CST_UDFCHAR06, 
                        src:CST_UDFCHAR07::varchar AS CST_UDFCHAR07, 
                        src:CST_UDFCHAR08::varchar AS CST_UDFCHAR08, 
                        src:CST_UDFCHAR09::varchar AS CST_UDFCHAR09, 
                        src:CST_UDFCHAR10::varchar AS CST_UDFCHAR10, 
                        src:CST_UDFCHAR11::varchar AS CST_UDFCHAR11, 
                        src:CST_UDFCHAR12::varchar AS CST_UDFCHAR12, 
                        src:CST_UDFCHAR13::varchar AS CST_UDFCHAR13, 
                        src:CST_UDFCHAR14::varchar AS CST_UDFCHAR14, 
                        src:CST_UDFCHAR15::varchar AS CST_UDFCHAR15, 
                        src:CST_UDFCHAR16::varchar AS CST_UDFCHAR16, 
                        src:CST_UDFCHAR17::varchar AS CST_UDFCHAR17, 
                        src:CST_UDFCHAR18::varchar AS CST_UDFCHAR18, 
                        src:CST_UDFCHAR19::varchar AS CST_UDFCHAR19, 
                        src:CST_UDFCHAR20::varchar AS CST_UDFCHAR20, 
                        src:CST_UDFCHAR21::varchar AS CST_UDFCHAR21, 
                        src:CST_UDFCHAR22::varchar AS CST_UDFCHAR22, 
                        src:CST_UDFCHAR23::varchar AS CST_UDFCHAR23, 
                        src:CST_UDFCHAR24::varchar AS CST_UDFCHAR24, 
                        src:CST_UDFCHAR25::varchar AS CST_UDFCHAR25, 
                        src:CST_UDFCHAR26::varchar AS CST_UDFCHAR26, 
                        src:CST_UDFCHAR27::varchar AS CST_UDFCHAR27, 
                        src:CST_UDFCHAR28::varchar AS CST_UDFCHAR28, 
                        src:CST_UDFCHAR29::varchar AS CST_UDFCHAR29, 
                        src:CST_UDFCHAR30::varchar AS CST_UDFCHAR30, 
                        src:CST_UDFCHKBOX01::varchar AS CST_UDFCHKBOX01, 
                        src:CST_UDFCHKBOX02::varchar AS CST_UDFCHKBOX02, 
                        src:CST_UDFCHKBOX03::varchar AS CST_UDFCHKBOX03, 
                        src:CST_UDFCHKBOX04::varchar AS CST_UDFCHKBOX04, 
                        src:CST_UDFCHKBOX05::varchar AS CST_UDFCHKBOX05, 
                        src:CST_UDFDATE01::datetime AS CST_UDFDATE01, 
                        src:CST_UDFDATE02::datetime AS CST_UDFDATE02, 
                        src:CST_UDFDATE03::datetime AS CST_UDFDATE03, 
                        src:CST_UDFDATE04::datetime AS CST_UDFDATE04, 
                        src:CST_UDFDATE05::datetime AS CST_UDFDATE05, 
                        src:CST_UDFNUM01::numeric(38, 10) AS CST_UDFNUM01, 
                        src:CST_UDFNUM02::numeric(38, 10) AS CST_UDFNUM02, 
                        src:CST_UDFNUM03::numeric(38, 10) AS CST_UDFNUM03, 
                        src:CST_UDFNUM04::numeric(38, 10) AS CST_UDFNUM04, 
                        src:CST_UDFNUM05::numeric(38, 10) AS CST_UDFNUM05, 
                        src:CST_UPDATECOUNT::numeric(38, 10) AS CST_UPDATECOUNT, 
                        src:CST_UPDATED::datetime AS CST_UPDATED, 
                        src:CST_UPDATEDBY::varchar AS CST_UPDATEDBY, 
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
    
                        
                src:CST_CODE,
            src:CST_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CST_CODE  ORDER BY 
            src:CST_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5COSTCODES as strm))