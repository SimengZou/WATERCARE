CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITYSAFETY AS SELECT
                        src:ESF_APPLYTOCHILDREN::varchar AS ESF_APPLYTOCHILDREN, 
                        src:ESF_CODE::varchar AS ESF_CODE, 
                        src:ESF_CREATED::datetime AS ESF_CREATED, 
                        src:ESF_CREATEDBY::varchar AS ESF_CREATEDBY, 
                        src:ESF_DELETEPENDING::varchar AS ESF_DELETEPENDING, 
                        src:ESF_FLAMMABILITY::varchar AS ESF_FLAMMABILITY, 
                        src:ESF_HAZARD::varchar AS ESF_HAZARD, 
                        src:ESF_HAZARD_ORG::varchar AS ESF_HAZARD_ORG, 
                        src:ESF_HEALTHHAZARD::varchar AS ESF_HEALTHHAZARD, 
                        src:ESF_INSTABILITY::varchar AS ESF_INSTABILITY, 
                        src:ESF_LASTSAVED::datetime AS ESF_LASTSAVED, 
                        src:ESF_PK::varchar AS ESF_PK, 
                        src:ESF_PRECAUTION::varchar AS ESF_PRECAUTION, 
                        src:ESF_PRECAUTION_ORG::varchar AS ESF_PRECAUTION_ORG, 
                        src:ESF_RENTCODE::varchar AS ESF_RENTCODE, 
                        src:ESF_RENTITY::varchar AS ESF_RENTITY, 
                        src:ESF_REVIEWED::datetime AS ESF_REVIEWED, 
                        src:ESF_REVIEWEDBY::varchar AS ESF_REVIEWEDBY, 
                        src:ESF_REVIEWEDNAME::varchar AS ESF_REVIEWEDNAME, 
                        src:ESF_REVIEWEDTYPE::varchar AS ESF_REVIEWEDTYPE, 
                        src:ESF_REVIEWTRIGGER::varchar AS ESF_REVIEWTRIGGER, 
                        src:ESF_SEQUENCE::numeric(38, 10) AS ESF_SEQUENCE, 
                        src:ESF_SPECIALHAZARDS::varchar AS ESF_SPECIALHAZARDS, 
                        src:ESF_TIMING::varchar AS ESF_TIMING, 
                        src:ESF_UDFCHAR01::varchar AS ESF_UDFCHAR01, 
                        src:ESF_UDFCHAR02::varchar AS ESF_UDFCHAR02, 
                        src:ESF_UDFCHAR03::varchar AS ESF_UDFCHAR03, 
                        src:ESF_UDFCHAR04::varchar AS ESF_UDFCHAR04, 
                        src:ESF_UDFCHAR05::varchar AS ESF_UDFCHAR05, 
                        src:ESF_UDFCHAR06::varchar AS ESF_UDFCHAR06, 
                        src:ESF_UDFCHAR07::varchar AS ESF_UDFCHAR07, 
                        src:ESF_UDFCHAR08::varchar AS ESF_UDFCHAR08, 
                        src:ESF_UDFCHAR09::varchar AS ESF_UDFCHAR09, 
                        src:ESF_UDFCHAR10::varchar AS ESF_UDFCHAR10, 
                        src:ESF_UDFCHAR11::varchar AS ESF_UDFCHAR11, 
                        src:ESF_UDFCHAR12::varchar AS ESF_UDFCHAR12, 
                        src:ESF_UDFCHAR13::varchar AS ESF_UDFCHAR13, 
                        src:ESF_UDFCHAR14::varchar AS ESF_UDFCHAR14, 
                        src:ESF_UDFCHAR15::varchar AS ESF_UDFCHAR15, 
                        src:ESF_UDFCHAR16::varchar AS ESF_UDFCHAR16, 
                        src:ESF_UDFCHAR17::varchar AS ESF_UDFCHAR17, 
                        src:ESF_UDFCHAR18::varchar AS ESF_UDFCHAR18, 
                        src:ESF_UDFCHAR19::varchar AS ESF_UDFCHAR19, 
                        src:ESF_UDFCHAR20::varchar AS ESF_UDFCHAR20, 
                        src:ESF_UDFCHAR21::varchar AS ESF_UDFCHAR21, 
                        src:ESF_UDFCHAR22::varchar AS ESF_UDFCHAR22, 
                        src:ESF_UDFCHAR23::varchar AS ESF_UDFCHAR23, 
                        src:ESF_UDFCHAR24::varchar AS ESF_UDFCHAR24, 
                        src:ESF_UDFCHAR25::varchar AS ESF_UDFCHAR25, 
                        src:ESF_UDFCHAR26::varchar AS ESF_UDFCHAR26, 
                        src:ESF_UDFCHAR27::varchar AS ESF_UDFCHAR27, 
                        src:ESF_UDFCHAR28::varchar AS ESF_UDFCHAR28, 
                        src:ESF_UDFCHAR29::varchar AS ESF_UDFCHAR29, 
                        src:ESF_UDFCHAR30::varchar AS ESF_UDFCHAR30, 
                        src:ESF_UDFCHKBOX01::varchar AS ESF_UDFCHKBOX01, 
                        src:ESF_UDFCHKBOX02::varchar AS ESF_UDFCHKBOX02, 
                        src:ESF_UDFCHKBOX03::varchar AS ESF_UDFCHKBOX03, 
                        src:ESF_UDFCHKBOX04::varchar AS ESF_UDFCHKBOX04, 
                        src:ESF_UDFCHKBOX05::varchar AS ESF_UDFCHKBOX05, 
                        src:ESF_UDFDATE01::datetime AS ESF_UDFDATE01, 
                        src:ESF_UDFDATE02::datetime AS ESF_UDFDATE02, 
                        src:ESF_UDFDATE03::datetime AS ESF_UDFDATE03, 
                        src:ESF_UDFDATE04::datetime AS ESF_UDFDATE04, 
                        src:ESF_UDFDATE05::datetime AS ESF_UDFDATE05, 
                        src:ESF_UDFNUM01::numeric(38, 10) AS ESF_UDFNUM01, 
                        src:ESF_UDFNUM02::numeric(38, 10) AS ESF_UDFNUM02, 
                        src:ESF_UDFNUM03::numeric(38, 10) AS ESF_UDFNUM03, 
                        src:ESF_UDFNUM04::numeric(38, 10) AS ESF_UDFNUM04, 
                        src:ESF_UDFNUM05::numeric(38, 10) AS ESF_UDFNUM05, 
                        src:ESF_UPDATECOUNT::numeric(38, 10) AS ESF_UPDATECOUNT, 
                        src:ESF_UPDATED::datetime AS ESF_UPDATED, 
                        src:ESF_UPDATEDBY::varchar AS ESF_UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ESF_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ESF_PK  ORDER BY 
            src:ESF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ENTITYSAFETY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_REVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_SEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ESF_LASTSAVED), '1900-01-01')) is not null