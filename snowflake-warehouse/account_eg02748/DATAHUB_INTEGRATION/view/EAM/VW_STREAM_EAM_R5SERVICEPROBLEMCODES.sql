CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SERVICEPROBLEMCODES AS SELECT
                        src:SPB_AUTOCREATEWO::varchar AS SPB_AUTOCREATEWO, 
                        src:SPB_AUTOREQUESTCASE::varchar AS SPB_AUTOREQUESTCASE, 
                        src:SPB_CASECLASS::varchar AS SPB_CASECLASS, 
                        src:SPB_CASECLASS_ORG::varchar AS SPB_CASECLASS_ORG, 
                        src:SPB_CASEFOLLOWUPREQUIRED::varchar AS SPB_CASEFOLLOWUPREQUIRED, 
                        src:SPB_CASEPRIORITY::varchar AS SPB_CASEPRIORITY, 
                        src:SPB_CASETYPE::varchar AS SPB_CASETYPE, 
                        src:SPB_CLASS::varchar AS SPB_CLASS, 
                        src:SPB_CLASS_ORG::varchar AS SPB_CLASS_ORG, 
                        src:SPB_CODE::varchar AS SPB_CODE, 
                        src:SPB_COSTCODE::varchar AS SPB_COSTCODE, 
                        src:SPB_DESC::varchar AS SPB_DESC, 
                        src:SPB_EQPUSABILITY::varchar AS SPB_EQPUSABILITY, 
                        src:SPB_EQPUSABILITY_ORG::varchar AS SPB_EQPUSABILITY_ORG, 
                        src:SPB_ESTHOURS::numeric(38, 10) AS SPB_ESTHOURS, 
                        src:SPB_FOLLOWUPSERVICECODE::varchar AS SPB_FOLLOWUPSERVICECODE, 
                        src:SPB_FOLLOWUPSERVICECODE_ORG::varchar AS SPB_FOLLOWUPSERVICECODE_ORG, 
                        src:SPB_FOLLOWUPTIMING::varchar AS SPB_FOLLOWUPTIMING, 
                        src:SPB_FORFOLLOWUP::varchar AS SPB_FORFOLLOWUP, 
                        src:SPB_GENERAL::varchar AS SPB_GENERAL, 
                        src:SPB_HAZARDMATERIAL::varchar AS SPB_HAZARDMATERIAL, 
                        src:SPB_JOBPLAN::varchar AS SPB_JOBPLAN, 
                        src:SPB_JOBTYPE::varchar AS SPB_JOBTYPE, 
                        src:SPB_LASTSAVED::datetime AS SPB_LASTSAVED, 
                        src:SPB_NOTUSED::varchar AS SPB_NOTUSED, 
                        src:SPB_OBJECT::varchar AS SPB_OBJECT, 
                        src:SPB_OBJECT_ORG::varchar AS SPB_OBJECT_ORG, 
                        src:SPB_ORG::varchar AS SPB_ORG, 
                        src:SPB_PENALTYFACTOR::numeric(38, 10) AS SPB_PENALTYFACTOR, 
                        src:SPB_PEOPLEREQ::numeric(38, 10) AS SPB_PEOPLEREQ, 
                        src:SPB_PERMFIXTURNAROUND::numeric(38, 10) AS SPB_PERMFIXTURNAROUND, 
                        src:SPB_PERMTURNAROUNDUNIT::varchar AS SPB_PERMTURNAROUNDUNIT, 
                        src:SPB_PRIORITY::varchar AS SPB_PRIORITY, 
                        src:SPB_REGULATORY::varchar AS SPB_REGULATORY, 
                        src:SPB_SERVICEREQUESTPORTAL::varchar AS SPB_SERVICEREQUESTPORTAL, 
                        src:SPB_STANDWORK::varchar AS SPB_STANDWORK, 
                        src:SPB_STDRESPTIME::numeric(38, 10) AS SPB_STDRESPTIME, 
                        src:SPB_STDRESPTIMEUNIT::varchar AS SPB_STDRESPTIMEUNIT, 
                        src:SPB_TASK::varchar AS SPB_TASK, 
                        src:SPB_TEMPFIXTURNAROUND::numeric(38, 10) AS SPB_TEMPFIXTURNAROUND, 
                        src:SPB_TEMPTURNAROUNDUNIT::varchar AS SPB_TEMPTURNAROUNDUNIT, 
                        src:SPB_TOTALESTCOST::numeric(38, 10) AS SPB_TOTALESTCOST, 
                        src:SPB_TRADE::varchar AS SPB_TRADE, 
                        src:SPB_UDFCHAR01::varchar AS SPB_UDFCHAR01, 
                        src:SPB_UDFCHAR02::varchar AS SPB_UDFCHAR02, 
                        src:SPB_UDFCHAR03::varchar AS SPB_UDFCHAR03, 
                        src:SPB_UDFCHAR04::varchar AS SPB_UDFCHAR04, 
                        src:SPB_UDFCHAR05::varchar AS SPB_UDFCHAR05, 
                        src:SPB_UDFCHAR06::varchar AS SPB_UDFCHAR06, 
                        src:SPB_UDFCHAR07::varchar AS SPB_UDFCHAR07, 
                        src:SPB_UDFCHAR08::varchar AS SPB_UDFCHAR08, 
                        src:SPB_UDFCHAR09::varchar AS SPB_UDFCHAR09, 
                        src:SPB_UDFCHAR10::varchar AS SPB_UDFCHAR10, 
                        src:SPB_UDFCHAR11::varchar AS SPB_UDFCHAR11, 
                        src:SPB_UDFCHAR12::varchar AS SPB_UDFCHAR12, 
                        src:SPB_UDFCHAR13::varchar AS SPB_UDFCHAR13, 
                        src:SPB_UDFCHAR14::varchar AS SPB_UDFCHAR14, 
                        src:SPB_UDFCHAR15::varchar AS SPB_UDFCHAR15, 
                        src:SPB_UDFCHAR16::varchar AS SPB_UDFCHAR16, 
                        src:SPB_UDFCHAR17::varchar AS SPB_UDFCHAR17, 
                        src:SPB_UDFCHAR18::varchar AS SPB_UDFCHAR18, 
                        src:SPB_UDFCHAR19::varchar AS SPB_UDFCHAR19, 
                        src:SPB_UDFCHAR20::varchar AS SPB_UDFCHAR20, 
                        src:SPB_UDFCHAR21::varchar AS SPB_UDFCHAR21, 
                        src:SPB_UDFCHAR22::varchar AS SPB_UDFCHAR22, 
                        src:SPB_UDFCHAR23::varchar AS SPB_UDFCHAR23, 
                        src:SPB_UDFCHAR24::varchar AS SPB_UDFCHAR24, 
                        src:SPB_UDFCHAR25::varchar AS SPB_UDFCHAR25, 
                        src:SPB_UDFCHAR26::varchar AS SPB_UDFCHAR26, 
                        src:SPB_UDFCHAR27::varchar AS SPB_UDFCHAR27, 
                        src:SPB_UDFCHAR28::varchar AS SPB_UDFCHAR28, 
                        src:SPB_UDFCHAR29::varchar AS SPB_UDFCHAR29, 
                        src:SPB_UDFCHAR30::varchar AS SPB_UDFCHAR30, 
                        src:SPB_UDFCHKBOX01::varchar AS SPB_UDFCHKBOX01, 
                        src:SPB_UDFCHKBOX02::varchar AS SPB_UDFCHKBOX02, 
                        src:SPB_UDFCHKBOX03::varchar AS SPB_UDFCHKBOX03, 
                        src:SPB_UDFCHKBOX04::varchar AS SPB_UDFCHKBOX04, 
                        src:SPB_UDFCHKBOX05::varchar AS SPB_UDFCHKBOX05, 
                        src:SPB_UDFDATE01::datetime AS SPB_UDFDATE01, 
                        src:SPB_UDFDATE02::datetime AS SPB_UDFDATE02, 
                        src:SPB_UDFDATE03::datetime AS SPB_UDFDATE03, 
                        src:SPB_UDFDATE04::datetime AS SPB_UDFDATE04, 
                        src:SPB_UDFDATE05::datetime AS SPB_UDFDATE05, 
                        src:SPB_UDFNUM01::numeric(38, 10) AS SPB_UDFNUM01, 
                        src:SPB_UDFNUM02::numeric(38, 10) AS SPB_UDFNUM02, 
                        src:SPB_UDFNUM03::numeric(38, 10) AS SPB_UDFNUM03, 
                        src:SPB_UDFNUM04::numeric(38, 10) AS SPB_UDFNUM04, 
                        src:SPB_UDFNUM05::numeric(38, 10) AS SPB_UDFNUM05, 
                        src:SPB_UPDATECOUNT::numeric(38, 10) AS SPB_UPDATECOUNT, 
                        src:SPB_WOCLASS::varchar AS SPB_WOCLASS, 
                        src:SPB_WOCLASS_ORG::varchar AS SPB_WOCLASS_ORG, 
                        src:SPB_WOSTATUS::varchar AS SPB_WOSTATUS, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:SPB_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:SPB_CODE,
                src:SPB_ORG  ORDER BY 
            src:SPB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SERVICEPROBLEMCODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_ESTHOURS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_PENALTYFACTOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_PEOPLEREQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_PERMFIXTURNAROUND), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_STDRESPTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_TEMPFIXTURNAROUND), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_TOTALESTCOST), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPB_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SPB_LASTSAVED), '1900-01-01')) is not null