CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCKLINES AS SELECT
                        src:DKL_ACD::numeric(38, 10) AS DKL_ACD, 
                        src:DKL_BIN::varchar AS DKL_BIN, 
                        src:DKL_CLASS::varchar AS DKL_CLASS, 
                        src:DKL_CLASS_ORG::varchar AS DKL_CLASS_ORG, 
                        src:DKL_COUNTED::varchar AS DKL_COUNTED, 
                        src:DKL_COUNTQTY::numeric(38, 10) AS DKL_COUNTQTY, 
                        src:DKL_DCKCODE::varchar AS DKL_DCKCODE, 
                        src:DKL_DIRECT::varchar AS DKL_DIRECT, 
                        src:DKL_INSPDATE::datetime AS DKL_INSPDATE, 
                        src:DKL_INSPECT::varchar AS DKL_INSPECT, 
                        src:DKL_INSPECTEDQTY::numeric(38, 10) AS DKL_INSPECTEDQTY, 
                        src:DKL_INSPECTOR::varchar AS DKL_INSPECTOR, 
                        src:DKL_INSPREJQTY::numeric(38, 10) AS DKL_INSPREJQTY, 
                        src:DKL_INSPRSTATUS::varchar AS DKL_INSPRSTATUS, 
                        src:DKL_INSPSTATUS::varchar AS DKL_INSPSTATUS, 
                        src:DKL_LASTSAVED::datetime AS DKL_LASTSAVED, 
                        src:DKL_LINE::numeric(38, 10) AS DKL_LINE, 
                        src:DKL_LINERSTATUS::varchar AS DKL_LINERSTATUS, 
                        src:DKL_LINESTATUS::varchar AS DKL_LINESTATUS, 
                        src:DKL_LOCATION::varchar AS DKL_LOCATION, 
                        src:DKL_LOT::varchar AS DKL_LOT, 
                        src:DKL_MANLOT::varchar AS DKL_MANLOT, 
                        src:DKL_MANLOTEXP::datetime AS DKL_MANLOTEXP, 
                        src:DKL_MANUFACTPART::varchar AS DKL_MANUFACTPART, 
                        src:DKL_MANUFACTURER::varchar AS DKL_MANUFACTURER, 
                        src:DKL_MOBILEDATEUPDATED::datetime AS DKL_MOBILEDATEUPDATED, 
                        src:DKL_MRC::varchar AS DKL_MRC, 
                        src:DKL_OBJECT::varchar AS DKL_OBJECT, 
                        src:DKL_OBJECT_ORG::varchar AS DKL_OBJECT_ORG, 
                        src:DKL_OBRTYPE::varchar AS DKL_OBRTYPE, 
                        src:DKL_OBTYPE::varchar AS DKL_OBTYPE, 
                        src:DKL_ORDER::varchar AS DKL_ORDER, 
                        src:DKL_ORDER_ORG::varchar AS DKL_ORDER_ORG, 
                        src:DKL_ORDLINE::numeric(38, 10) AS DKL_ORDLINE, 
                        src:DKL_PART::varchar AS DKL_PART, 
                        src:DKL_PART_ORG::varchar AS DKL_PART_ORG, 
                        src:DKL_PERSON::varchar AS DKL_PERSON, 
                        src:DKL_PRINT::numeric(38, 10) AS DKL_PRINT, 
                        src:DKL_RECVCODE::varchar AS DKL_RECVCODE, 
                        src:DKL_RECVQTY::numeric(38, 10) AS DKL_RECVQTY, 
                        src:DKL_REJREASON::varchar AS DKL_REJREASON, 
                        src:DKL_REPAIRCONDITION::varchar AS DKL_REPAIRCONDITION, 
                        src:DKL_REPAIRPRICE::numeric(38, 10) AS DKL_REPAIRPRICE, 
                        src:DKL_RETNCODE::varchar AS DKL_RETNCODE, 
                        src:DKL_RETNQTY::numeric(38, 10) AS DKL_RETNQTY, 
                        src:DKL_SCRAPQTY::numeric(38, 10) AS DKL_SCRAPQTY, 
                        src:DKL_SERIAL::varchar AS DKL_SERIAL, 
                        src:DKL_UDFCHAR01::varchar AS DKL_UDFCHAR01, 
                        src:DKL_UDFCHAR02::varchar AS DKL_UDFCHAR02, 
                        src:DKL_UDFCHAR03::varchar AS DKL_UDFCHAR03, 
                        src:DKL_UDFCHAR04::varchar AS DKL_UDFCHAR04, 
                        src:DKL_UDFCHAR05::varchar AS DKL_UDFCHAR05, 
                        src:DKL_UDFCHAR06::varchar AS DKL_UDFCHAR06, 
                        src:DKL_UDFCHAR07::varchar AS DKL_UDFCHAR07, 
                        src:DKL_UDFCHAR08::varchar AS DKL_UDFCHAR08, 
                        src:DKL_UDFCHAR09::varchar AS DKL_UDFCHAR09, 
                        src:DKL_UDFCHAR10::varchar AS DKL_UDFCHAR10, 
                        src:DKL_UDFCHAR11::varchar AS DKL_UDFCHAR11, 
                        src:DKL_UDFCHAR12::varchar AS DKL_UDFCHAR12, 
                        src:DKL_UDFCHAR13::varchar AS DKL_UDFCHAR13, 
                        src:DKL_UDFCHAR14::varchar AS DKL_UDFCHAR14, 
                        src:DKL_UDFCHAR15::varchar AS DKL_UDFCHAR15, 
                        src:DKL_UDFCHAR16::varchar AS DKL_UDFCHAR16, 
                        src:DKL_UDFCHAR17::varchar AS DKL_UDFCHAR17, 
                        src:DKL_UDFCHAR18::varchar AS DKL_UDFCHAR18, 
                        src:DKL_UDFCHAR19::varchar AS DKL_UDFCHAR19, 
                        src:DKL_UDFCHAR20::varchar AS DKL_UDFCHAR20, 
                        src:DKL_UDFCHAR21::varchar AS DKL_UDFCHAR21, 
                        src:DKL_UDFCHAR22::varchar AS DKL_UDFCHAR22, 
                        src:DKL_UDFCHAR23::varchar AS DKL_UDFCHAR23, 
                        src:DKL_UDFCHAR24::varchar AS DKL_UDFCHAR24, 
                        src:DKL_UDFCHAR25::varchar AS DKL_UDFCHAR25, 
                        src:DKL_UDFCHAR26::varchar AS DKL_UDFCHAR26, 
                        src:DKL_UDFCHAR27::varchar AS DKL_UDFCHAR27, 
                        src:DKL_UDFCHAR28::varchar AS DKL_UDFCHAR28, 
                        src:DKL_UDFCHAR29::varchar AS DKL_UDFCHAR29, 
                        src:DKL_UDFCHAR30::varchar AS DKL_UDFCHAR30, 
                        src:DKL_UDFCHKBOX01::varchar AS DKL_UDFCHKBOX01, 
                        src:DKL_UDFCHKBOX02::varchar AS DKL_UDFCHKBOX02, 
                        src:DKL_UDFCHKBOX03::varchar AS DKL_UDFCHKBOX03, 
                        src:DKL_UDFCHKBOX04::varchar AS DKL_UDFCHKBOX04, 
                        src:DKL_UDFCHKBOX05::varchar AS DKL_UDFCHKBOX05, 
                        src:DKL_UDFDATE01::datetime AS DKL_UDFDATE01, 
                        src:DKL_UDFDATE02::datetime AS DKL_UDFDATE02, 
                        src:DKL_UDFDATE03::datetime AS DKL_UDFDATE03, 
                        src:DKL_UDFDATE04::datetime AS DKL_UDFDATE04, 
                        src:DKL_UDFDATE05::datetime AS DKL_UDFDATE05, 
                        src:DKL_UDFNUM01::numeric(38, 10) AS DKL_UDFNUM01, 
                        src:DKL_UDFNUM02::numeric(38, 10) AS DKL_UDFNUM02, 
                        src:DKL_UDFNUM03::numeric(38, 10) AS DKL_UDFNUM03, 
                        src:DKL_UDFNUM04::numeric(38, 10) AS DKL_UDFNUM04, 
                        src:DKL_UDFNUM05::numeric(38, 10) AS DKL_UDFNUM05, 
                        src:DKL_UPDATECOUNT::numeric(38, 10) AS DKL_UPDATECOUNT, 
                        src:DKL_UPDATED::datetime AS DKL_UPDATED, 
                        src:DKL_UPDUSER::varchar AS DKL_UPDUSER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DKL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DKL_DCKCODE,
                src:DKL_LINE  ORDER BY 
            src:DKL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCKLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_ACD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_COUNTQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_INSPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_INSPECTEDQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_INSPREJQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_MANLOTEXP), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_MOBILEDATEUPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_ORDLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_PRINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_RECVQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_REPAIRPRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_RETNQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_SCRAPQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DKL_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DKL_LASTSAVED), '1900-01-01')) is not null