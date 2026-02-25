CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5STANDWORKS AS SELECT
                        src:STW_CLASS::varchar AS STW_CLASS, 
                        src:STW_CLASS_ORG::varchar AS STW_CLASS_ORG, 
                        src:STW_CODE::varchar AS STW_CODE, 
                        src:STW_CREATED::datetime AS STW_CREATED, 
                        src:STW_CREATEDBY::varchar AS STW_CREATEDBY, 
                        src:STW_DESC::varchar AS STW_DESC, 
                        src:STW_DURATION::numeric(38, 10) AS STW_DURATION, 
                        src:STW_JOBTYPE::varchar AS STW_JOBTYPE, 
                        src:STW_LASTSAVED::datetime AS STW_LASTSAVED, 
                        src:STW_NOTUSED::varchar AS STW_NOTUSED, 
                        src:STW_OBCLASS::varchar AS STW_OBCLASS, 
                        src:STW_OBCLASS_ORG::varchar AS STW_OBCLASS_ORG, 
                        src:STW_OBJECT::varchar AS STW_OBJECT, 
                        src:STW_OBJECT_ORG::varchar AS STW_OBJECT_ORG, 
                        src:STW_OBRTYPE::varchar AS STW_OBRTYPE, 
                        src:STW_OBTYPE::varchar AS STW_OBTYPE, 
                        src:STW_ORG::varchar AS STW_ORG, 
                        src:STW_PERMITREVIEWED::datetime AS STW_PERMITREVIEWED, 
                        src:STW_PERMITREVIEWEDBY::varchar AS STW_PERMITREVIEWEDBY, 
                        src:STW_PERMITREVIEWEDNAME::varchar AS STW_PERMITREVIEWEDNAME, 
                        src:STW_PERMITREVIEWEDTYPE::varchar AS STW_PERMITREVIEWEDTYPE, 
                        src:STW_PERMITREVIEWREQUIRED::datetime AS STW_PERMITREVIEWREQUIRED, 
                        src:STW_PRIORITY::varchar AS STW_PRIORITY, 
                        src:STW_REQM::varchar AS STW_REQM, 
                        src:STW_SAFETYREVIEWED::datetime AS STW_SAFETYREVIEWED, 
                        src:STW_SAFETYREVIEWEDBY::varchar AS STW_SAFETYREVIEWEDBY, 
                        src:STW_SAFETYREVIEWEDNAME::varchar AS STW_SAFETYREVIEWEDNAME, 
                        src:STW_SAFETYREVIEWEDTYPE::varchar AS STW_SAFETYREVIEWEDTYPE, 
                        src:STW_SAFETYREVIEWREQUIRED::datetime AS STW_SAFETYREVIEWREQUIRED, 
                        src:STW_TYPE::varchar AS STW_TYPE, 
                        src:STW_UDFCHAR01::varchar AS STW_UDFCHAR01, 
                        src:STW_UDFCHAR02::varchar AS STW_UDFCHAR02, 
                        src:STW_UDFCHAR03::varchar AS STW_UDFCHAR03, 
                        src:STW_UDFCHAR04::varchar AS STW_UDFCHAR04, 
                        src:STW_UDFCHAR05::varchar AS STW_UDFCHAR05, 
                        src:STW_UDFCHAR06::varchar AS STW_UDFCHAR06, 
                        src:STW_UDFCHAR07::varchar AS STW_UDFCHAR07, 
                        src:STW_UDFCHAR08::varchar AS STW_UDFCHAR08, 
                        src:STW_UDFCHAR09::varchar AS STW_UDFCHAR09, 
                        src:STW_UDFCHAR10::varchar AS STW_UDFCHAR10, 
                        src:STW_UDFCHAR11::varchar AS STW_UDFCHAR11, 
                        src:STW_UDFCHAR12::varchar AS STW_UDFCHAR12, 
                        src:STW_UDFCHAR13::varchar AS STW_UDFCHAR13, 
                        src:STW_UDFCHAR14::varchar AS STW_UDFCHAR14, 
                        src:STW_UDFCHAR15::varchar AS STW_UDFCHAR15, 
                        src:STW_UDFCHAR16::varchar AS STW_UDFCHAR16, 
                        src:STW_UDFCHAR17::varchar AS STW_UDFCHAR17, 
                        src:STW_UDFCHAR18::varchar AS STW_UDFCHAR18, 
                        src:STW_UDFCHAR19::varchar AS STW_UDFCHAR19, 
                        src:STW_UDFCHAR20::varchar AS STW_UDFCHAR20, 
                        src:STW_UDFCHAR21::varchar AS STW_UDFCHAR21, 
                        src:STW_UDFCHAR22::varchar AS STW_UDFCHAR22, 
                        src:STW_UDFCHAR23::varchar AS STW_UDFCHAR23, 
                        src:STW_UDFCHAR24::varchar AS STW_UDFCHAR24, 
                        src:STW_UDFCHAR25::varchar AS STW_UDFCHAR25, 
                        src:STW_UDFCHAR26::varchar AS STW_UDFCHAR26, 
                        src:STW_UDFCHAR27::varchar AS STW_UDFCHAR27, 
                        src:STW_UDFCHAR28::varchar AS STW_UDFCHAR28, 
                        src:STW_UDFCHAR29::varchar AS STW_UDFCHAR29, 
                        src:STW_UDFCHAR30::varchar AS STW_UDFCHAR30, 
                        src:STW_UDFCHAR31::varchar AS STW_UDFCHAR31, 
                        src:STW_UDFCHAR32::varchar AS STW_UDFCHAR32, 
                        src:STW_UDFCHAR33::varchar AS STW_UDFCHAR33, 
                        src:STW_UDFCHAR34::varchar AS STW_UDFCHAR34, 
                        src:STW_UDFCHAR35::varchar AS STW_UDFCHAR35, 
                        src:STW_UDFCHAR36::varchar AS STW_UDFCHAR36, 
                        src:STW_UDFCHAR37::varchar AS STW_UDFCHAR37, 
                        src:STW_UDFCHAR38::varchar AS STW_UDFCHAR38, 
                        src:STW_UDFCHAR39::varchar AS STW_UDFCHAR39, 
                        src:STW_UDFCHAR40::varchar AS STW_UDFCHAR40, 
                        src:STW_UDFCHAR41::varchar AS STW_UDFCHAR41, 
                        src:STW_UDFCHAR42::varchar AS STW_UDFCHAR42, 
                        src:STW_UDFCHAR43::varchar AS STW_UDFCHAR43, 
                        src:STW_UDFCHAR44::varchar AS STW_UDFCHAR44, 
                        src:STW_UDFCHAR45::varchar AS STW_UDFCHAR45, 
                        src:STW_UDFCHAR46::varchar AS STW_UDFCHAR46, 
                        src:STW_UDFCHAR47::varchar AS STW_UDFCHAR47, 
                        src:STW_UDFCHAR48::varchar AS STW_UDFCHAR48, 
                        src:STW_UDFCHAR49::varchar AS STW_UDFCHAR49, 
                        src:STW_UDFCHAR50::varchar AS STW_UDFCHAR50, 
                        src:STW_UDFCHAR51::varchar AS STW_UDFCHAR51, 
                        src:STW_UDFCHAR52::varchar AS STW_UDFCHAR52, 
                        src:STW_UDFCHAR53::varchar AS STW_UDFCHAR53, 
                        src:STW_UDFCHAR54::varchar AS STW_UDFCHAR54, 
                        src:STW_UDFCHAR55::varchar AS STW_UDFCHAR55, 
                        src:STW_UDFCHKBOX01::varchar AS STW_UDFCHKBOX01, 
                        src:STW_UDFCHKBOX02::varchar AS STW_UDFCHKBOX02, 
                        src:STW_UDFCHKBOX03::varchar AS STW_UDFCHKBOX03, 
                        src:STW_UDFCHKBOX04::varchar AS STW_UDFCHKBOX04, 
                        src:STW_UDFCHKBOX05::varchar AS STW_UDFCHKBOX05, 
                        src:STW_UDFCHKBOX06::varchar AS STW_UDFCHKBOX06, 
                        src:STW_UDFCHKBOX07::varchar AS STW_UDFCHKBOX07, 
                        src:STW_UDFCHKBOX08::varchar AS STW_UDFCHKBOX08, 
                        src:STW_UDFCHKBOX09::varchar AS STW_UDFCHKBOX09, 
                        src:STW_UDFCHKBOX10::varchar AS STW_UDFCHKBOX10, 
                        src:STW_UDFDATE01::datetime AS STW_UDFDATE01, 
                        src:STW_UDFDATE02::datetime AS STW_UDFDATE02, 
                        src:STW_UDFDATE03::datetime AS STW_UDFDATE03, 
                        src:STW_UDFDATE04::datetime AS STW_UDFDATE04, 
                        src:STW_UDFDATE05::datetime AS STW_UDFDATE05, 
                        src:STW_UDFDATE06::datetime AS STW_UDFDATE06, 
                        src:STW_UDFDATE07::datetime AS STW_UDFDATE07, 
                        src:STW_UDFDATE08::datetime AS STW_UDFDATE08, 
                        src:STW_UDFDATE09::datetime AS STW_UDFDATE09, 
                        src:STW_UDFDATE10::datetime AS STW_UDFDATE10, 
                        src:STW_UDFNOTE01::varchar AS STW_UDFNOTE01, 
                        src:STW_UDFNOTE02::varchar AS STW_UDFNOTE02, 
                        src:STW_UDFNUM01::numeric(38, 10) AS STW_UDFNUM01, 
                        src:STW_UDFNUM02::numeric(38, 10) AS STW_UDFNUM02, 
                        src:STW_UDFNUM03::numeric(38, 10) AS STW_UDFNUM03, 
                        src:STW_UDFNUM04::numeric(38, 10) AS STW_UDFNUM04, 
                        src:STW_UDFNUM05::numeric(38, 10) AS STW_UDFNUM05, 
                        src:STW_UDFNUM06::numeric(38, 10) AS STW_UDFNUM06, 
                        src:STW_UDFNUM07::numeric(38, 10) AS STW_UDFNUM07, 
                        src:STW_UDFNUM08::numeric(38, 10) AS STW_UDFNUM08, 
                        src:STW_UDFNUM09::numeric(38, 10) AS STW_UDFNUM09, 
                        src:STW_UDFNUM10::numeric(38, 10) AS STW_UDFNUM10, 
                        src:STW_UPDATECOUNT::numeric(38, 10) AS STW_UPDATECOUNT, 
                        src:STW_UPDATED::datetime AS STW_UPDATED, 
                        src:STW_UPDATEDBY::varchar AS STW_UPDATEDBY, 
                        src:STW_WOCLASS::varchar AS STW_WOCLASS, 
                        src:STW_WOCLASS_ORG::varchar AS STW_WOCLASS_ORG, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:STW_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:STW_CODE  ORDER BY 
            src:STW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STANDWORKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_PERMITREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_PERMITREVIEWREQUIRED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_SAFETYREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_SAFETYREVIEWREQUIRED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE06), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE07), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE08), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE09), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UDFDATE10), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM06), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM07), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM08), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM09), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UDFNUM10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STW_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STW_LASTSAVED), '1900-01-01')) is not null