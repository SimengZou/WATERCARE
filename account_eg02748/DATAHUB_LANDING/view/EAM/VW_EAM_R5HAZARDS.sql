CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HAZARDS AS SELECT
                        src:HAZ_APPROVED::datetime AS HAZ_APPROVED, 
                        src:HAZ_APPROVEDBY::varchar AS HAZ_APPROVEDBY, 
                        src:HAZ_CLASS::varchar AS HAZ_CLASS, 
                        src:HAZ_CLASS_ORG::varchar AS HAZ_CLASS_ORG, 
                        src:HAZ_CODE::varchar AS HAZ_CODE, 
                        src:HAZ_CREATED::datetime AS HAZ_CREATED, 
                        src:HAZ_CREATEDBY::varchar AS HAZ_CREATEDBY, 
                        src:HAZ_DESC::varchar AS HAZ_DESC, 
                        src:HAZ_LASTSAVED::datetime AS HAZ_LASTSAVED, 
                        src:HAZ_NOTUSED::varchar AS HAZ_NOTUSED, 
                        src:HAZ_ORG::varchar AS HAZ_ORG, 
                        src:HAZ_REQUESTED::datetime AS HAZ_REQUESTED, 
                        src:HAZ_REQUESTEDBY::varchar AS HAZ_REQUESTEDBY, 
                        src:HAZ_REVIEWREQUIRED::datetime AS HAZ_REVIEWREQUIRED, 
                        src:HAZ_REVISION::numeric(38, 10) AS HAZ_REVISION, 
                        src:HAZ_REVISIONREASON::varchar AS HAZ_REVISIONREASON, 
                        src:HAZ_RSTATUS::varchar AS HAZ_RSTATUS, 
                        src:HAZ_STATUS::varchar AS HAZ_STATUS, 
                        src:HAZ_TYPE::varchar AS HAZ_TYPE, 
                        src:HAZ_UDFCHAR01::varchar AS HAZ_UDFCHAR01, 
                        src:HAZ_UDFCHAR02::varchar AS HAZ_UDFCHAR02, 
                        src:HAZ_UDFCHAR03::varchar AS HAZ_UDFCHAR03, 
                        src:HAZ_UDFCHAR04::varchar AS HAZ_UDFCHAR04, 
                        src:HAZ_UDFCHAR05::varchar AS HAZ_UDFCHAR05, 
                        src:HAZ_UDFCHAR06::varchar AS HAZ_UDFCHAR06, 
                        src:HAZ_UDFCHAR07::varchar AS HAZ_UDFCHAR07, 
                        src:HAZ_UDFCHAR08::varchar AS HAZ_UDFCHAR08, 
                        src:HAZ_UDFCHAR09::varchar AS HAZ_UDFCHAR09, 
                        src:HAZ_UDFCHAR10::varchar AS HAZ_UDFCHAR10, 
                        src:HAZ_UDFCHAR11::varchar AS HAZ_UDFCHAR11, 
                        src:HAZ_UDFCHAR12::varchar AS HAZ_UDFCHAR12, 
                        src:HAZ_UDFCHAR13::varchar AS HAZ_UDFCHAR13, 
                        src:HAZ_UDFCHAR14::varchar AS HAZ_UDFCHAR14, 
                        src:HAZ_UDFCHAR15::varchar AS HAZ_UDFCHAR15, 
                        src:HAZ_UDFCHAR16::varchar AS HAZ_UDFCHAR16, 
                        src:HAZ_UDFCHAR17::varchar AS HAZ_UDFCHAR17, 
                        src:HAZ_UDFCHAR18::varchar AS HAZ_UDFCHAR18, 
                        src:HAZ_UDFCHAR19::varchar AS HAZ_UDFCHAR19, 
                        src:HAZ_UDFCHAR20::varchar AS HAZ_UDFCHAR20, 
                        src:HAZ_UDFCHAR21::varchar AS HAZ_UDFCHAR21, 
                        src:HAZ_UDFCHAR22::varchar AS HAZ_UDFCHAR22, 
                        src:HAZ_UDFCHAR23::varchar AS HAZ_UDFCHAR23, 
                        src:HAZ_UDFCHAR24::varchar AS HAZ_UDFCHAR24, 
                        src:HAZ_UDFCHAR25::varchar AS HAZ_UDFCHAR25, 
                        src:HAZ_UDFCHAR26::varchar AS HAZ_UDFCHAR26, 
                        src:HAZ_UDFCHAR27::varchar AS HAZ_UDFCHAR27, 
                        src:HAZ_UDFCHAR28::varchar AS HAZ_UDFCHAR28, 
                        src:HAZ_UDFCHAR29::varchar AS HAZ_UDFCHAR29, 
                        src:HAZ_UDFCHAR30::varchar AS HAZ_UDFCHAR30, 
                        src:HAZ_UDFCHKBOX01::varchar AS HAZ_UDFCHKBOX01, 
                        src:HAZ_UDFCHKBOX02::varchar AS HAZ_UDFCHKBOX02, 
                        src:HAZ_UDFCHKBOX03::varchar AS HAZ_UDFCHKBOX03, 
                        src:HAZ_UDFCHKBOX04::varchar AS HAZ_UDFCHKBOX04, 
                        src:HAZ_UDFCHKBOX05::varchar AS HAZ_UDFCHKBOX05, 
                        src:HAZ_UDFDATE01::datetime AS HAZ_UDFDATE01, 
                        src:HAZ_UDFDATE02::datetime AS HAZ_UDFDATE02, 
                        src:HAZ_UDFDATE03::datetime AS HAZ_UDFDATE03, 
                        src:HAZ_UDFDATE04::datetime AS HAZ_UDFDATE04, 
                        src:HAZ_UDFDATE05::datetime AS HAZ_UDFDATE05, 
                        src:HAZ_UDFNUM01::numeric(38, 10) AS HAZ_UDFNUM01, 
                        src:HAZ_UDFNUM02::numeric(38, 10) AS HAZ_UDFNUM02, 
                        src:HAZ_UDFNUM03::numeric(38, 10) AS HAZ_UDFNUM03, 
                        src:HAZ_UDFNUM04::numeric(38, 10) AS HAZ_UDFNUM04, 
                        src:HAZ_UDFNUM05::numeric(38, 10) AS HAZ_UDFNUM05, 
                        src:HAZ_UPDATECOUNT::numeric(38, 10) AS HAZ_UPDATECOUNT, 
                        src:HAZ_UPDATED::datetime AS HAZ_UPDATED, 
                        src:HAZ_UPDATEDBY::varchar AS HAZ_UPDATEDBY, 
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
    
                        
                src:HAZ_CODE,
                src:HAZ_ORG,
                src:HAZ_REVISION,
            src:HAZ_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HAZ_CODE,
                src:HAZ_ORG,
                src:HAZ_REVISION  ORDER BY 
            src:HAZ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HAZARDS as strm))