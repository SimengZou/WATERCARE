CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WORKSAFETY AS SELECT
                        src:KSF_CODE::varchar AS KSF_CODE, 
                        src:KSF_CREATED::datetime AS KSF_CREATED, 
                        src:KSF_CREATEDBY::varchar AS KSF_CREATEDBY, 
                        src:KSF_DELETEPENDING::varchar AS KSF_DELETEPENDING, 
                        src:KSF_FLAMMABILITY::varchar AS KSF_FLAMMABILITY, 
                        src:KSF_HAZARD::varchar AS KSF_HAZARD, 
                        src:KSF_HAZARDREV::numeric(38, 10) AS KSF_HAZARDREV, 
                        src:KSF_HAZARD_ORG::varchar AS KSF_HAZARD_ORG, 
                        src:KSF_HEALTHHAZARD::varchar AS KSF_HEALTHHAZARD, 
                        src:KSF_INSTABILITY::varchar AS KSF_INSTABILITY, 
                        src:KSF_LASTSAVED::datetime AS KSF_LASTSAVED, 
                        src:KSF_PK::varchar AS KSF_PK, 
                        src:KSF_PRECAUTION::varchar AS KSF_PRECAUTION, 
                        src:KSF_PRECAUTIONAPPLIED::varchar AS KSF_PRECAUTIONAPPLIED, 
                        src:KSF_PRECAUTIONREV::numeric(38, 10) AS KSF_PRECAUTIONREV, 
                        src:KSF_PRECAUTION_ORG::varchar AS KSF_PRECAUTION_ORG, 
                        src:KSF_RENTITY::varchar AS KSF_RENTITY, 
                        src:KSF_RESPONSIBILITY::varchar AS KSF_RESPONSIBILITY, 
                        src:KSF_REVIEWED::datetime AS KSF_REVIEWED, 
                        src:KSF_REVIEWEDBY::varchar AS KSF_REVIEWEDBY, 
                        src:KSF_REVIEWEDNAME::varchar AS KSF_REVIEWEDNAME, 
                        src:KSF_REVIEWEDTYPE::varchar AS KSF_REVIEWEDTYPE, 
                        src:KSF_SEQUENCE::numeric(38, 10) AS KSF_SEQUENCE, 
                        src:KSF_SOURCECODE::varchar AS KSF_SOURCECODE, 
                        src:KSF_SOURCEORG::varchar AS KSF_SOURCEORG, 
                        src:KSF_SOURCERENTITY::varchar AS KSF_SOURCERENTITY, 
                        src:KSF_SOURCEUPDATED::datetime AS KSF_SOURCEUPDATED, 
                        src:KSF_SOURCEUPDATEDBY::varchar AS KSF_SOURCEUPDATEDBY, 
                        src:KSF_SPECIALHAZARDS::varchar AS KSF_SPECIALHAZARDS, 
                        src:KSF_TIMING::varchar AS KSF_TIMING, 
                        src:KSF_UDFCHAR01::varchar AS KSF_UDFCHAR01, 
                        src:KSF_UDFCHAR02::varchar AS KSF_UDFCHAR02, 
                        src:KSF_UDFCHAR03::varchar AS KSF_UDFCHAR03, 
                        src:KSF_UDFCHAR04::varchar AS KSF_UDFCHAR04, 
                        src:KSF_UDFCHAR05::varchar AS KSF_UDFCHAR05, 
                        src:KSF_UDFCHAR06::varchar AS KSF_UDFCHAR06, 
                        src:KSF_UDFCHAR07::varchar AS KSF_UDFCHAR07, 
                        src:KSF_UDFCHAR08::varchar AS KSF_UDFCHAR08, 
                        src:KSF_UDFCHAR09::varchar AS KSF_UDFCHAR09, 
                        src:KSF_UDFCHAR10::varchar AS KSF_UDFCHAR10, 
                        src:KSF_UDFCHAR11::varchar AS KSF_UDFCHAR11, 
                        src:KSF_UDFCHAR12::varchar AS KSF_UDFCHAR12, 
                        src:KSF_UDFCHAR13::varchar AS KSF_UDFCHAR13, 
                        src:KSF_UDFCHAR14::varchar AS KSF_UDFCHAR14, 
                        src:KSF_UDFCHAR15::varchar AS KSF_UDFCHAR15, 
                        src:KSF_UDFCHAR16::varchar AS KSF_UDFCHAR16, 
                        src:KSF_UDFCHAR17::varchar AS KSF_UDFCHAR17, 
                        src:KSF_UDFCHAR18::varchar AS KSF_UDFCHAR18, 
                        src:KSF_UDFCHAR19::varchar AS KSF_UDFCHAR19, 
                        src:KSF_UDFCHAR20::varchar AS KSF_UDFCHAR20, 
                        src:KSF_UDFCHAR21::varchar AS KSF_UDFCHAR21, 
                        src:KSF_UDFCHAR22::varchar AS KSF_UDFCHAR22, 
                        src:KSF_UDFCHAR23::varchar AS KSF_UDFCHAR23, 
                        src:KSF_UDFCHAR24::varchar AS KSF_UDFCHAR24, 
                        src:KSF_UDFCHAR25::varchar AS KSF_UDFCHAR25, 
                        src:KSF_UDFCHAR26::varchar AS KSF_UDFCHAR26, 
                        src:KSF_UDFCHAR27::varchar AS KSF_UDFCHAR27, 
                        src:KSF_UDFCHAR28::varchar AS KSF_UDFCHAR28, 
                        src:KSF_UDFCHAR29::varchar AS KSF_UDFCHAR29, 
                        src:KSF_UDFCHAR30::varchar AS KSF_UDFCHAR30, 
                        src:KSF_UDFCHKBOX01::varchar AS KSF_UDFCHKBOX01, 
                        src:KSF_UDFCHKBOX02::varchar AS KSF_UDFCHKBOX02, 
                        src:KSF_UDFCHKBOX03::varchar AS KSF_UDFCHKBOX03, 
                        src:KSF_UDFCHKBOX04::varchar AS KSF_UDFCHKBOX04, 
                        src:KSF_UDFCHKBOX05::varchar AS KSF_UDFCHKBOX05, 
                        src:KSF_UDFDATE01::datetime AS KSF_UDFDATE01, 
                        src:KSF_UDFDATE02::datetime AS KSF_UDFDATE02, 
                        src:KSF_UDFDATE03::datetime AS KSF_UDFDATE03, 
                        src:KSF_UDFDATE04::datetime AS KSF_UDFDATE04, 
                        src:KSF_UDFDATE05::datetime AS KSF_UDFDATE05, 
                        src:KSF_UDFNUM01::numeric(38, 10) AS KSF_UDFNUM01, 
                        src:KSF_UDFNUM02::numeric(38, 10) AS KSF_UDFNUM02, 
                        src:KSF_UDFNUM03::numeric(38, 10) AS KSF_UDFNUM03, 
                        src:KSF_UDFNUM04::numeric(38, 10) AS KSF_UDFNUM04, 
                        src:KSF_UDFNUM05::numeric(38, 10) AS KSF_UDFNUM05, 
                        src:KSF_UPDATECOUNT::numeric(38, 10) AS KSF_UPDATECOUNT, 
                        src:KSF_UPDATED::datetime AS KSF_UPDATED, 
                        src:KSF_UPDATEDBY::varchar AS KSF_UPDATEDBY, 
                        src:KSF_VALIDUNTIL::datetime AS KSF_VALIDUNTIL, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:KSF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:KSF_PK  ORDER BY 
            src:KSF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WORKSAFETY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_HAZARDREV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_PRECAUTIONREV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_REVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_SEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_SOURCEUPDATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KSF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_VALIDUNTIL), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:KSF_LASTSAVED), '1900-01-01')) is not null