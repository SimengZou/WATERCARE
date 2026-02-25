CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PPMS AS SELECT
                        src:PPM_CLASS::varchar AS PPM_CLASS, 
                        src:PPM_CLASS_ORG::varchar AS PPM_CLASS_ORG, 
                        src:PPM_CODE::varchar AS PPM_CODE, 
                        src:PPM_DESC::varchar AS PPM_DESC, 
                        src:PPM_DURATION::numeric(38, 10) AS PPM_DURATION, 
                        src:PPM_ESTWORKLOAD::numeric(38, 10) AS PPM_ESTWORKLOAD, 
                        src:PPM_FREQ::numeric(38, 10) AS PPM_FREQ, 
                        src:PPM_GENWINDOW::numeric(38, 10) AS PPM_GENWINDOW, 
                        src:PPM_ISSTYPE::varchar AS PPM_ISSTYPE, 
                        src:PPM_JOBTYPE::varchar AS PPM_JOBTYPE, 
                        src:PPM_LASTSAVED::datetime AS PPM_LASTSAVED, 
                        src:PPM_METER::numeric(38, 10) AS PPM_METER, 
                        src:PPM_METER2::numeric(38, 10) AS PPM_METER2, 
                        src:PPM_METUOM::varchar AS PPM_METUOM, 
                        src:PPM_METUOM2::varchar AS PPM_METUOM2, 
                        src:PPM_NEARWINDOW::numeric(38, 10) AS PPM_NEARWINDOW, 
                        src:PPM_NESTED::varchar AS PPM_NESTED, 
                        src:PPM_NESTEDPMSTATUS::varchar AS PPM_NESTEDPMSTATUS, 
                        src:PPM_NESTEDTOLMAX::numeric(38, 10) AS PPM_NESTEDTOLMAX, 
                        src:PPM_NESTEDTOLMET2MAX::numeric(38, 10) AS PPM_NESTEDTOLMET2MAX, 
                        src:PPM_NESTEDTOLMET2MIN::numeric(38, 10) AS PPM_NESTEDTOLMET2MIN, 
                        src:PPM_NESTEDTOLMETMAX::numeric(38, 10) AS PPM_NESTEDTOLMETMAX, 
                        src:PPM_NESTEDTOLMETMIN::numeric(38, 10) AS PPM_NESTEDTOLMETMIN, 
                        src:PPM_NESTEDTOLMIN::numeric(38, 10) AS PPM_NESTEDTOLMIN, 
                        src:PPM_NOTUSED::varchar AS PPM_NOTUSED, 
                        src:PPM_OKWINDOW::numeric(38, 10) AS PPM_OKWINDOW, 
                        src:PPM_ORG::varchar AS PPM_ORG, 
                        src:PPM_PACKAGE::varchar AS PPM_PACKAGE, 
                        src:PPM_PERFORMONDAY::numeric(38, 10) AS PPM_PERFORMONDAY, 
                        src:PPM_PERFORMONWEEK::varchar AS PPM_PERFORMONWEEK, 
                        src:PPM_PERIODUOM::varchar AS PPM_PERIODUOM, 
                        src:PPM_PERMITREVIEWED::datetime AS PPM_PERMITREVIEWED, 
                        src:PPM_PERMITREVIEWEDBY::varchar AS PPM_PERMITREVIEWEDBY, 
                        src:PPM_PERMITREVIEWEDNAME::varchar AS PPM_PERMITREVIEWEDNAME, 
                        src:PPM_PERMITREVIEWEDTYPE::varchar AS PPM_PERMITREVIEWEDTYPE, 
                        src:PPM_PERMITREVIEWREQUIRED::datetime AS PPM_PERMITREVIEWREQUIRED, 
                        src:PPM_PLAN::varchar AS PPM_PLAN, 
                        src:PPM_PRIORITY::varchar AS PPM_PRIORITY, 
                        src:PPM_PRODPRIORITY::varchar AS PPM_PRODPRIORITY, 
                        src:PPM_REQUESTEDENDDATEBUFFER::numeric(38, 10) AS PPM_REQUESTEDENDDATEBUFFER, 
                        src:PPM_REQUESTEDSTARTDATEBUFFER::numeric(38, 10) AS PPM_REQUESTEDSTARTDATEBUFFER, 
                        src:PPM_REVISION::numeric(38, 10) AS PPM_REVISION, 
                        src:PPM_REVRSTATUS::varchar AS PPM_REVRSTATUS, 
                        src:PPM_REVSTATUS::varchar AS PPM_REVSTATUS, 
                        src:PPM_SAFETYREVIEWED::datetime AS PPM_SAFETYREVIEWED, 
                        src:PPM_SAFETYREVIEWEDBY::varchar AS PPM_SAFETYREVIEWEDBY, 
                        src:PPM_SAFETYREVIEWEDNAME::varchar AS PPM_SAFETYREVIEWEDNAME, 
                        src:PPM_SAFETYREVIEWEDTYPE::varchar AS PPM_SAFETYREVIEWEDTYPE, 
                        src:PPM_SAFETYREVIEWREQUIRED::datetime AS PPM_SAFETYREVIEWREQUIRED, 
                        src:PPM_SCHEDGRP::varchar AS PPM_SCHEDGRP, 
                        src:PPM_SPECIAL::varchar AS PPM_SPECIAL, 
                        src:PPM_UDFCHAR01::varchar AS PPM_UDFCHAR01, 
                        src:PPM_UDFCHAR02::varchar AS PPM_UDFCHAR02, 
                        src:PPM_UDFCHAR03::varchar AS PPM_UDFCHAR03, 
                        src:PPM_UDFCHAR04::varchar AS PPM_UDFCHAR04, 
                        src:PPM_UDFCHAR05::varchar AS PPM_UDFCHAR05, 
                        src:PPM_UDFCHAR06::varchar AS PPM_UDFCHAR06, 
                        src:PPM_UDFCHAR07::varchar AS PPM_UDFCHAR07, 
                        src:PPM_UDFCHAR08::varchar AS PPM_UDFCHAR08, 
                        src:PPM_UDFCHAR09::varchar AS PPM_UDFCHAR09, 
                        src:PPM_UDFCHAR10::varchar AS PPM_UDFCHAR10, 
                        src:PPM_UDFCHAR11::varchar AS PPM_UDFCHAR11, 
                        src:PPM_UDFCHAR12::varchar AS PPM_UDFCHAR12, 
                        src:PPM_UDFCHAR13::varchar AS PPM_UDFCHAR13, 
                        src:PPM_UDFCHAR14::varchar AS PPM_UDFCHAR14, 
                        src:PPM_UDFCHAR15::varchar AS PPM_UDFCHAR15, 
                        src:PPM_UDFCHAR16::varchar AS PPM_UDFCHAR16, 
                        src:PPM_UDFCHAR17::varchar AS PPM_UDFCHAR17, 
                        src:PPM_UDFCHAR18::varchar AS PPM_UDFCHAR18, 
                        src:PPM_UDFCHAR19::varchar AS PPM_UDFCHAR19, 
                        src:PPM_UDFCHAR20::varchar AS PPM_UDFCHAR20, 
                        src:PPM_UDFCHAR21::varchar AS PPM_UDFCHAR21, 
                        src:PPM_UDFCHAR22::varchar AS PPM_UDFCHAR22, 
                        src:PPM_UDFCHAR23::varchar AS PPM_UDFCHAR23, 
                        src:PPM_UDFCHAR24::varchar AS PPM_UDFCHAR24, 
                        src:PPM_UDFCHAR25::varchar AS PPM_UDFCHAR25, 
                        src:PPM_UDFCHAR26::varchar AS PPM_UDFCHAR26, 
                        src:PPM_UDFCHAR27::varchar AS PPM_UDFCHAR27, 
                        src:PPM_UDFCHAR28::varchar AS PPM_UDFCHAR28, 
                        src:PPM_UDFCHAR29::varchar AS PPM_UDFCHAR29, 
                        src:PPM_UDFCHAR30::varchar AS PPM_UDFCHAR30, 
                        src:PPM_UDFCHAR31::varchar AS PPM_UDFCHAR31, 
                        src:PPM_UDFCHAR32::varchar AS PPM_UDFCHAR32, 
                        src:PPM_UDFCHAR33::varchar AS PPM_UDFCHAR33, 
                        src:PPM_UDFCHAR34::varchar AS PPM_UDFCHAR34, 
                        src:PPM_UDFCHAR35::varchar AS PPM_UDFCHAR35, 
                        src:PPM_UDFCHAR36::varchar AS PPM_UDFCHAR36, 
                        src:PPM_UDFCHAR37::varchar AS PPM_UDFCHAR37, 
                        src:PPM_UDFCHAR38::varchar AS PPM_UDFCHAR38, 
                        src:PPM_UDFCHAR39::varchar AS PPM_UDFCHAR39, 
                        src:PPM_UDFCHAR40::varchar AS PPM_UDFCHAR40, 
                        src:PPM_UDFCHAR41::varchar AS PPM_UDFCHAR41, 
                        src:PPM_UDFCHAR42::varchar AS PPM_UDFCHAR42, 
                        src:PPM_UDFCHAR43::varchar AS PPM_UDFCHAR43, 
                        src:PPM_UDFCHAR44::varchar AS PPM_UDFCHAR44, 
                        src:PPM_UDFCHAR45::varchar AS PPM_UDFCHAR45, 
                        src:PPM_UDFCHKBOX01::varchar AS PPM_UDFCHKBOX01, 
                        src:PPM_UDFCHKBOX02::varchar AS PPM_UDFCHKBOX02, 
                        src:PPM_UDFCHKBOX03::varchar AS PPM_UDFCHKBOX03, 
                        src:PPM_UDFCHKBOX04::varchar AS PPM_UDFCHKBOX04, 
                        src:PPM_UDFCHKBOX05::varchar AS PPM_UDFCHKBOX05, 
                        src:PPM_UDFCHKBOX06::varchar AS PPM_UDFCHKBOX06, 
                        src:PPM_UDFCHKBOX07::varchar AS PPM_UDFCHKBOX07, 
                        src:PPM_UDFCHKBOX08::varchar AS PPM_UDFCHKBOX08, 
                        src:PPM_UDFCHKBOX09::varchar AS PPM_UDFCHKBOX09, 
                        src:PPM_UDFCHKBOX10::varchar AS PPM_UDFCHKBOX10, 
                        src:PPM_UDFDATE01::datetime AS PPM_UDFDATE01, 
                        src:PPM_UDFDATE02::datetime AS PPM_UDFDATE02, 
                        src:PPM_UDFDATE03::datetime AS PPM_UDFDATE03, 
                        src:PPM_UDFDATE04::datetime AS PPM_UDFDATE04, 
                        src:PPM_UDFDATE05::datetime AS PPM_UDFDATE05, 
                        src:PPM_UDFDATE06::datetime AS PPM_UDFDATE06, 
                        src:PPM_UDFDATE07::datetime AS PPM_UDFDATE07, 
                        src:PPM_UDFDATE08::datetime AS PPM_UDFDATE08, 
                        src:PPM_UDFDATE09::datetime AS PPM_UDFDATE09, 
                        src:PPM_UDFDATE10::datetime AS PPM_UDFDATE10, 
                        src:PPM_UDFNOTE01::varchar AS PPM_UDFNOTE01, 
                        src:PPM_UDFNOTE02::varchar AS PPM_UDFNOTE02, 
                        src:PPM_UDFNUM01::numeric(38, 10) AS PPM_UDFNUM01, 
                        src:PPM_UDFNUM02::numeric(38, 10) AS PPM_UDFNUM02, 
                        src:PPM_UDFNUM03::numeric(38, 10) AS PPM_UDFNUM03, 
                        src:PPM_UDFNUM04::numeric(38, 10) AS PPM_UDFNUM04, 
                        src:PPM_UDFNUM05::numeric(38, 10) AS PPM_UDFNUM05, 
                        src:PPM_UDFNUM06::numeric(38, 10) AS PPM_UDFNUM06, 
                        src:PPM_UDFNUM07::numeric(38, 10) AS PPM_UDFNUM07, 
                        src:PPM_UDFNUM08::numeric(38, 10) AS PPM_UDFNUM08, 
                        src:PPM_UDFNUM09::numeric(38, 10) AS PPM_UDFNUM09, 
                        src:PPM_UDFNUM10::numeric(38, 10) AS PPM_UDFNUM10, 
                        src:PPM_UPDATECOUNT::numeric(38, 10) AS PPM_UPDATECOUNT, 
                        src:PPM_WOCLASS::varchar AS PPM_WOCLASS, 
                        src:PPM_WOCLASS_ORG::varchar AS PPM_WOCLASS_ORG, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PPM_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PPM_CODE,
                src:PPM_REVISION  ORDER BY 
            src:PPM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_ESTWORKLOAD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_FREQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_GENWINDOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_METER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_METER2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NEARWINDOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMET2MAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMET2MIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMETMAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMETMIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_NESTEDTOLMIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_OKWINDOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_PERFORMONDAY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_PERMITREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_PERMITREVIEWREQUIRED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_REQUESTEDENDDATEBUFFER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_REQUESTEDSTARTDATEBUFFER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_SAFETYREVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_SAFETYREVIEWREQUIRED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE06), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE07), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE08), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE09), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_UDFDATE10), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM06), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM07), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM08), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM09), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UDFNUM10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPM_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPM_LASTSAVED), '1900-01-01')) is not null