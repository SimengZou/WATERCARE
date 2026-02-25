CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TASKS AS SELECT
                        src:TSK_ACTIVECHECKLIST::varchar AS TSK_ACTIVECHECKLIST, 
                        src:TSK_ASMLEVEL::varchar AS TSK_ASMLEVEL, 
                        src:TSK_BUYER::varchar AS TSK_BUYER, 
                        src:TSK_CASEMANAGEMENTCHECKLIST::varchar AS TSK_CASEMANAGEMENTCHECKLIST, 
                        src:TSK_CHECKLISTPERFBYREQ::varchar AS TSK_CHECKLISTPERFBYREQ, 
                        src:TSK_CHECKLISTREJECTEDWOSTATUS::varchar AS TSK_CHECKLISTREJECTEDWOSTATUS, 
                        src:TSK_CHECKLISTREVBYREQ::varchar AS TSK_CHECKLISTREVBYREQ, 
                        src:TSK_CLASS::varchar AS TSK_CLASS, 
                        src:TSK_CLASS_ORG::varchar AS TSK_CLASS_ORG, 
                        src:TSK_CODE::varchar AS TSK_CODE, 
                        src:TSK_COMMODITY::varchar AS TSK_COMMODITY, 
                        src:TSK_COMPLEVEL::varchar AS TSK_COMPLEVEL, 
                        src:TSK_COMPLOCATION::varchar AS TSK_COMPLOCATION, 
                        src:TSK_CREATED::datetime AS TSK_CREATED, 
                        src:TSK_DEFAULTTAG::varchar AS TSK_DEFAULTTAG, 
                        src:TSK_DESC::varchar AS TSK_DESC, 
                        src:TSK_DISCONNECTED_CHKLIST::varchar AS TSK_DISCONNECTED_CHKLIST, 
                        src:TSK_ENABLEENHANCEDPLANNING::varchar AS TSK_ENABLEENHANCEDPLANNING, 
                        src:TSK_HOURS::numeric(38, 10) AS TSK_HOURS, 
                        src:TSK_ISOLATIONMETHOD::varchar AS TSK_ISOLATIONMETHOD, 
                        src:TSK_JOBPLAN::varchar AS TSK_JOBPLAN, 
                        src:TSK_JOBPLANTYPE::varchar AS TSK_JOBPLANTYPE, 
                        src:TSK_LASTSAVED::datetime AS TSK_LASTSAVED, 
                        src:TSK_MANUFACT::varchar AS TSK_MANUFACT, 
                        src:TSK_MATLIST::varchar AS TSK_MATLIST, 
                        src:TSK_MULTIPLETRADES::varchar AS TSK_MULTIPLETRADES, 
                        src:TSK_NONCONFORMITYCHECKLIST::varchar AS TSK_NONCONFORMITYCHECKLIST, 
                        src:TSK_NOTUSED::varchar AS TSK_NOTUSED, 
                        src:TSK_OBJCLASS::varchar AS TSK_OBJCLASS, 
                        src:TSK_OBJCLASS_ORG::varchar AS TSK_OBJCLASS_ORG, 
                        src:TSK_OBJTYPE::varchar AS TSK_OBJTYPE, 
                        src:TSK_ORG::varchar AS TSK_ORG, 
                        src:TSK_PERFORMBY2RESP::varchar AS TSK_PERFORMBY2RESP, 
                        src:TSK_PERFORMBYRESP::varchar AS TSK_PERFORMBYRESP, 
                        src:TSK_PERFORMEDBYWOSTATUS::varchar AS TSK_PERFORMEDBYWOSTATUS, 
                        src:TSK_PERSONS::numeric(38, 10) AS TSK_PERSONS, 
                        src:TSK_PLANNINGLEVEL::varchar AS TSK_PLANNINGLEVEL, 
                        src:TSK_PLANNINGRECORDSEXIST::varchar AS TSK_PLANNINGRECORDSEXIST, 
                        src:TSK_PREFSUP::varchar AS TSK_PREFSUP, 
                        src:TSK_PREFSUP_ORG::varchar AS TSK_PREFSUP_ORG, 
                        src:TSK_PREVENTPERFORMEDBYSIGN::varchar AS TSK_PREVENTPERFORMEDBYSIGN, 
                        src:TSK_PREVENTREVIEWEDBYSIGN::varchar AS TSK_PREVENTREVIEWEDBYSIGN, 
                        src:TSK_PRICE::numeric(38, 10) AS TSK_PRICE, 
                        src:TSK_REUSABLE::varchar AS TSK_REUSABLE, 
                        src:TSK_REVIEWEDBYWOSTATUS::varchar AS TSK_REVIEWEDBYWOSTATUS, 
                        src:TSK_REVIEWRESP::varchar AS TSK_REVIEWRESP, 
                        src:TSK_REVISION::numeric(38, 10) AS TSK_REVISION, 
                        src:TSK_REVRSTATUS::varchar AS TSK_REVRSTATUS, 
                        src:TSK_REVSTATUS::varchar AS TSK_REVSTATUS, 
                        src:TSK_RPC::varchar AS TSK_RPC, 
                        src:TSK_SYSLEVEL::varchar AS TSK_SYSLEVEL, 
                        src:TSK_TAGHEADER::varchar AS TSK_TAGHEADER, 
                        src:TSK_TAGLINE1::varchar AS TSK_TAGLINE1, 
                        src:TSK_TAGLINE2::varchar AS TSK_TAGLINE2, 
                        src:TSK_TAGLINE3::varchar AS TSK_TAGLINE3, 
                        src:TSK_TAGLINE4::varchar AS TSK_TAGLINE4, 
                        src:TSK_TPF::varchar AS TSK_TPF, 
                        src:TSK_TRADE::varchar AS TSK_TRADE, 
                        src:TSK_TYPE::varchar AS TSK_TYPE, 
                        src:TSK_UDFCHAR01::varchar AS TSK_UDFCHAR01, 
                        src:TSK_UDFCHAR02::varchar AS TSK_UDFCHAR02, 
                        src:TSK_UDFCHAR03::varchar AS TSK_UDFCHAR03, 
                        src:TSK_UDFCHAR04::varchar AS TSK_UDFCHAR04, 
                        src:TSK_UDFCHAR05::varchar AS TSK_UDFCHAR05, 
                        src:TSK_UDFCHAR06::varchar AS TSK_UDFCHAR06, 
                        src:TSK_UDFCHAR07::varchar AS TSK_UDFCHAR07, 
                        src:TSK_UDFCHAR08::varchar AS TSK_UDFCHAR08, 
                        src:TSK_UDFCHAR09::varchar AS TSK_UDFCHAR09, 
                        src:TSK_UDFCHAR10::varchar AS TSK_UDFCHAR10, 
                        src:TSK_UDFCHAR11::varchar AS TSK_UDFCHAR11, 
                        src:TSK_UDFCHAR12::varchar AS TSK_UDFCHAR12, 
                        src:TSK_UDFCHAR13::varchar AS TSK_UDFCHAR13, 
                        src:TSK_UDFCHAR14::varchar AS TSK_UDFCHAR14, 
                        src:TSK_UDFCHAR15::varchar AS TSK_UDFCHAR15, 
                        src:TSK_UDFCHAR16::varchar AS TSK_UDFCHAR16, 
                        src:TSK_UDFCHAR17::varchar AS TSK_UDFCHAR17, 
                        src:TSK_UDFCHAR18::varchar AS TSK_UDFCHAR18, 
                        src:TSK_UDFCHAR19::varchar AS TSK_UDFCHAR19, 
                        src:TSK_UDFCHAR20::varchar AS TSK_UDFCHAR20, 
                        src:TSK_UDFCHAR21::varchar AS TSK_UDFCHAR21, 
                        src:TSK_UDFCHAR22::varchar AS TSK_UDFCHAR22, 
                        src:TSK_UDFCHAR23::varchar AS TSK_UDFCHAR23, 
                        src:TSK_UDFCHAR24::varchar AS TSK_UDFCHAR24, 
                        src:TSK_UDFCHAR25::varchar AS TSK_UDFCHAR25, 
                        src:TSK_UDFCHAR26::varchar AS TSK_UDFCHAR26, 
                        src:TSK_UDFCHAR27::varchar AS TSK_UDFCHAR27, 
                        src:TSK_UDFCHAR28::varchar AS TSK_UDFCHAR28, 
                        src:TSK_UDFCHAR29::varchar AS TSK_UDFCHAR29, 
                        src:TSK_UDFCHAR30::varchar AS TSK_UDFCHAR30, 
                        src:TSK_UDFCHKBOX01::varchar AS TSK_UDFCHKBOX01, 
                        src:TSK_UDFCHKBOX02::varchar AS TSK_UDFCHKBOX02, 
                        src:TSK_UDFCHKBOX03::varchar AS TSK_UDFCHKBOX03, 
                        src:TSK_UDFCHKBOX04::varchar AS TSK_UDFCHKBOX04, 
                        src:TSK_UDFCHKBOX05::varchar AS TSK_UDFCHKBOX05, 
                        src:TSK_UDFDATE01::datetime AS TSK_UDFDATE01, 
                        src:TSK_UDFDATE02::datetime AS TSK_UDFDATE02, 
                        src:TSK_UDFDATE03::datetime AS TSK_UDFDATE03, 
                        src:TSK_UDFDATE04::datetime AS TSK_UDFDATE04, 
                        src:TSK_UDFDATE05::datetime AS TSK_UDFDATE05, 
                        src:TSK_UDFNUM01::numeric(38, 10) AS TSK_UDFNUM01, 
                        src:TSK_UDFNUM02::numeric(38, 10) AS TSK_UDFNUM02, 
                        src:TSK_UDFNUM03::numeric(38, 10) AS TSK_UDFNUM03, 
                        src:TSK_UDFNUM04::numeric(38, 10) AS TSK_UDFNUM04, 
                        src:TSK_UDFNUM05::numeric(38, 10) AS TSK_UDFNUM05, 
                        src:TSK_UOM::varchar AS TSK_UOM, 
                        src:TSK_UPDATECOUNT::numeric(38, 10) AS TSK_UPDATECOUNT, 
                        src:TSK_UPDATED::datetime AS TSK_UPDATED, 
                        src:TSK_VIEWONLYRESP::varchar AS TSK_VIEWONLYRESP, 
                        src:TSK_WAP::varchar AS TSK_WAP, 
                        src:TSK_WOCLASS::varchar AS TSK_WOCLASS, 
                        src:TSK_WOCLASS_ORG::varchar AS TSK_WOCLASS_ORG, 
                        src:TSK_WODESC::varchar AS TSK_WODESC, 
                        src:TSK_WOPRIORITY::varchar AS TSK_WOPRIORITY, 
                        src:TSK_WORKSPACEMOVES::varchar AS TSK_WORKSPACEMOVES, 
                        src:TSK_WOSTATUS::varchar AS TSK_WOSTATUS, 
                        src:TSK_WOTYPE::varchar AS TSK_WOTYPE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TSK_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TSK_CODE,
                src:TSK_REVISION  ORDER BY 
            src:TSK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TASKS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_HOURS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_PERSONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_PRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TSK_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TSK_LASTSAVED), '1900-01-01')) is not null