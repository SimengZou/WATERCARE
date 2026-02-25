CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTIVITIES AS SELECT
                        src:ACT_ACT::numeric(38, 10) AS ACT_ACT, 
                        src:ACT_ASMLEVEL::varchar AS ACT_ASMLEVEL, 
                        src:ACT_ASSIGNMENTSTATUS::varchar AS ACT_ASSIGNMENTSTATUS, 
                        src:ACT_CLASS::varchar AS ACT_CLASS, 
                        src:ACT_CLASS_ORG::varchar AS ACT_CLASS_ORG, 
                        src:ACT_COMPLETED::varchar AS ACT_COMPLETED, 
                        src:ACT_COMPLEVEL::varchar AS ACT_COMPLEVEL, 
                        src:ACT_COMPLOCATION::varchar AS ACT_COMPLOCATION, 
                        src:ACT_CONDITION_AS_FOUND::varchar AS ACT_CONDITION_AS_FOUND, 
                        src:ACT_DEFERMAINTENANCE::varchar AS ACT_DEFERMAINTENANCE, 
                        src:ACT_DEFERREDIRECTMATS::varchar AS ACT_DEFERREDIRECTMATS, 
                        src:ACT_DEFERREDORIGACT::numeric(38, 10) AS ACT_DEFERREDORIGACT, 
                        src:ACT_DEFERREDORIGWO::varchar AS ACT_DEFERREDORIGWO, 
                        src:ACT_DURATION::numeric(38, 10) AS ACT_DURATION, 
                        src:ACT_EST::numeric(38, 10) AS ACT_EST, 
                        src:ACT_ESTLABORCOST::numeric(38, 10) AS ACT_ESTLABORCOST, 
                        src:ACT_ESTMATLCOST::numeric(38, 10) AS ACT_ESTMATLCOST, 
                        src:ACT_ESTMISCCOST::numeric(38, 10) AS ACT_ESTMISCCOST, 
                        src:ACT_EVENT::varchar AS ACT_EVENT, 
                        src:ACT_FIXH::varchar AS ACT_FIXH, 
                        src:ACT_FROMGEOREF::varchar AS ACT_FROMGEOREF, 
                        src:ACT_FROMPOINT::numeric(38, 10) AS ACT_FROMPOINT, 
                        src:ACT_FROMREFDESC::varchar AS ACT_FROMREFDESC, 
                        src:ACT_FROM_HORIZONTAL_OFFSET::numeric(38, 10) AS ACT_FROM_HORIZONTAL_OFFSET, 
                        src:ACT_FROM_HORIZONTAL_OFFSETTYPE::varchar AS ACT_FROM_HORIZONTAL_OFFSETTYPE, 
                        src:ACT_FROM_HORIZONTAL_OFFSETUOM::varchar AS ACT_FROM_HORIZONTAL_OFFSETUOM, 
                        src:ACT_FROM_LATITUDE::numeric(38, 10) AS ACT_FROM_LATITUDE, 
                        src:ACT_FROM_LONGITUDE::numeric(38, 10) AS ACT_FROM_LONGITUDE, 
                        src:ACT_FROM_OFFSET::numeric(38, 10) AS ACT_FROM_OFFSET, 
                        src:ACT_FROM_OFFSET_DIRECTION::varchar AS ACT_FROM_OFFSET_DIRECTION, 
                        src:ACT_FROM_OFFSET_PERCENTAGE::numeric(38, 10) AS ACT_FROM_OFFSET_PERCENTAGE, 
                        src:ACT_FROM_REFERENCE::numeric(38, 10) AS ACT_FROM_REFERENCE, 
                        src:ACT_FROM_RELATIONSHIP_TYPE::varchar AS ACT_FROM_RELATIONSHIP_TYPE, 
                        src:ACT_FROM_VERTICAL_OFFSET::numeric(38, 10) AS ACT_FROM_VERTICAL_OFFSET, 
                        src:ACT_FROM_VERTICAL_OFFSETTYPE::varchar AS ACT_FROM_VERTICAL_OFFSETTYPE, 
                        src:ACT_FROM_VERTICAL_OFFSETUOM::varchar AS ACT_FROM_VERTICAL_OFFSETUOM, 
                        src:ACT_FROM_XCOORDINATE::numeric(38, 10) AS ACT_FROM_XCOORDINATE, 
                        src:ACT_FROM_YCOORDINATE::numeric(38, 10) AS ACT_FROM_YCOORDINATE, 
                        src:ACT_GRAPH::varchar AS ACT_GRAPH, 
                        src:ACT_HIRE::varchar AS ACT_HIRE, 
                        src:ACT_INVOICE::varchar AS ACT_INVOICE, 
                        src:ACT_INVOICELINE::numeric(38, 10) AS ACT_INVOICELINE, 
                        src:ACT_INVOICE_ORG::varchar AS ACT_INVOICE_ORG, 
                        src:ACT_INVOICE_REVISION::numeric(38, 10) AS ACT_INVOICE_REVISION, 
                        src:ACT_LASTSAVED::datetime AS ACT_LASTSAVED, 
                        src:ACT_LATESTSCHED::datetime AS ACT_LATESTSCHED, 
                        src:ACT_LEVEL::numeric(38, 10) AS ACT_LEVEL, 
                        src:ACT_MANUFACT::varchar AS ACT_MANUFACT, 
                        src:ACT_MATLIST::varchar AS ACT_MATLIST, 
                        src:ACT_MATLREV::numeric(38, 10) AS ACT_MATLREV, 
                        src:ACT_MAXDUR::numeric(38, 10) AS ACT_MAXDUR, 
                        src:ACT_MINHOURS::varchar AS ACT_MINHOURS, 
                        src:ACT_MRC::varchar AS ACT_MRC, 
                        src:ACT_MULTIPLETRADES::varchar AS ACT_MULTIPLETRADES, 
                        src:ACT_NEWDUR::numeric(38, 10) AS ACT_NEWDUR, 
                        src:ACT_NEWSTART::datetime AS ACT_NEWSTART, 
                        src:ACT_NOTE::varchar AS ACT_NOTE, 
                        src:ACT_NT::numeric(38, 10) AS ACT_NT, 
                        src:ACT_NTRATE::numeric(38, 10) AS ACT_NTRATE, 
                        src:ACT_ORDER::varchar AS ACT_ORDER, 
                        src:ACT_ORDERED::varchar AS ACT_ORDERED, 
                        src:ACT_ORDER_ORG::varchar AS ACT_ORDER_ORG, 
                        src:ACT_ORDLINE::numeric(38, 10) AS ACT_ORDLINE, 
                        src:ACT_ORDRTYPE::varchar AS ACT_ORDRTYPE, 
                        src:ACT_ORDTYPE::varchar AS ACT_ORDTYPE, 
                        src:ACT_OT::numeric(38, 10) AS ACT_OT, 
                        src:ACT_OTRATE::numeric(38, 10) AS ACT_OTRATE, 
                        src:ACT_PARENT::numeric(38, 10) AS ACT_PARENT, 
                        src:ACT_PERCOMPLETE::numeric(38, 10) AS ACT_PERCOMPLETE, 
                        src:ACT_PERFORMBY2RESP::varchar AS ACT_PERFORMBY2RESP, 
                        src:ACT_PERFORMBYRESP::varchar AS ACT_PERFORMBYRESP, 
                        src:ACT_PERFORMED::datetime AS ACT_PERFORMED, 
                        src:ACT_PERFORMED2::datetime AS ACT_PERFORMED2, 
                        src:ACT_PERFORMEDBY::varchar AS ACT_PERFORMEDBY, 
                        src:ACT_PERFORMEDBY2::varchar AS ACT_PERFORMEDBY2, 
                        src:ACT_PERFORMEDTYPE::varchar AS ACT_PERFORMEDTYPE, 
                        src:ACT_PERFORMEDTYPE2::varchar AS ACT_PERFORMEDTYPE2, 
                        src:ACT_PERRESP::varchar AS ACT_PERRESP, 
                        src:ACT_PERSONS::numeric(38, 10) AS ACT_PERSONS, 
                        src:ACT_PLANNINGLEVEL::varchar AS ACT_PLANNINGLEVEL, 
                        src:ACT_PROJBUD::varchar AS ACT_PROJBUD, 
                        src:ACT_PROJECT::varchar AS ACT_PROJECT, 
                        src:ACT_PURRATE::numeric(38, 10) AS ACT_PURRATE, 
                        src:ACT_QTY::numeric(38, 10) AS ACT_QTY, 
                        src:ACT_REJECTPERFORMEDBY::varchar AS ACT_REJECTPERFORMEDBY, 
                        src:ACT_REJECTPERFORMEDBY2::varchar AS ACT_REJECTPERFORMEDBY2, 
                        src:ACT_REJECTREASON::varchar AS ACT_REJECTREASON, 
                        src:ACT_RELATEDWO::varchar AS ACT_RELATEDWO, 
                        src:ACT_REM::numeric(38, 10) AS ACT_REM, 
                        src:ACT_REQ::varchar AS ACT_REQ, 
                        src:ACT_REQLINE::numeric(38, 10) AS ACT_REQLINE, 
                        src:ACT_REVIEWED::datetime AS ACT_REVIEWED, 
                        src:ACT_REVIEWEDBY::varchar AS ACT_REVIEWEDBY, 
                        src:ACT_REVIEWEDTYPE::varchar AS ACT_REVIEWEDTYPE, 
                        src:ACT_REVIEWRESP::varchar AS ACT_REVIEWRESP, 
                        src:ACT_ROUTE::varchar AS ACT_ROUTE, 
                        src:ACT_RPC::varchar AS ACT_RPC, 
                        src:ACT_SCHEDHRS::numeric(38, 10) AS ACT_SCHEDHRS, 
                        src:ACT_SCHEDULINGSESSIONID::numeric(38, 10) AS ACT_SCHEDULINGSESSIONID, 
                        src:ACT_SCHEDULINGSESSIONTYPE::varchar AS ACT_SCHEDULINGSESSIONTYPE, 
                        src:ACT_SEQ::numeric(38, 10) AS ACT_SEQ, 
                        src:ACT_SHIFT::varchar AS ACT_SHIFT, 
                        src:ACT_SOURCECODE::varchar AS ACT_SOURCECODE, 
                        src:ACT_SOURCESYSTEM::varchar AS ACT_SOURCESYSTEM, 
                        src:ACT_SPECIAL::varchar AS ACT_SPECIAL, 
                        src:ACT_START::datetime AS ACT_START, 
                        src:ACT_SUPPLIER::varchar AS ACT_SUPPLIER, 
                        src:ACT_SUPPLIER_ORG::varchar AS ACT_SUPPLIER_ORG, 
                        src:ACT_SYSLEVEL::varchar AS ACT_SYSLEVEL, 
                        src:ACT_TASK::varchar AS ACT_TASK, 
                        src:ACT_TASKREV::numeric(38, 10) AS ACT_TASKREV, 
                        src:ACT_TIME::numeric(38, 10) AS ACT_TIME, 
                        src:ACT_TOGEOREF::varchar AS ACT_TOGEOREF, 
                        src:ACT_TOPOINT::numeric(38, 10) AS ACT_TOPOINT, 
                        src:ACT_TOPPARENT::numeric(38, 10) AS ACT_TOPPARENT, 
                        src:ACT_TOREFDESC::varchar AS ACT_TOREFDESC, 
                        src:ACT_TO_HORIZONTAL_OFFSET::numeric(38, 10) AS ACT_TO_HORIZONTAL_OFFSET, 
                        src:ACT_TO_HORIZONTAL_OFFSETTYPE::varchar AS ACT_TO_HORIZONTAL_OFFSETTYPE, 
                        src:ACT_TO_HORIZONTAL_OFFSETUOM::varchar AS ACT_TO_HORIZONTAL_OFFSETUOM, 
                        src:ACT_TO_LATITUDE::numeric(38, 10) AS ACT_TO_LATITUDE, 
                        src:ACT_TO_LONGITUDE::numeric(38, 10) AS ACT_TO_LONGITUDE, 
                        src:ACT_TO_OFFSET::numeric(38, 10) AS ACT_TO_OFFSET, 
                        src:ACT_TO_OFFSET_DIRECTION::varchar AS ACT_TO_OFFSET_DIRECTION, 
                        src:ACT_TO_OFFSET_PERCENTAGE::numeric(38, 10) AS ACT_TO_OFFSET_PERCENTAGE, 
                        src:ACT_TO_REFERENCE::numeric(38, 10) AS ACT_TO_REFERENCE, 
                        src:ACT_TO_RELATIONSHIP_TYPE::varchar AS ACT_TO_RELATIONSHIP_TYPE, 
                        src:ACT_TO_VERTICAL_OFFSET::numeric(38, 10) AS ACT_TO_VERTICAL_OFFSET, 
                        src:ACT_TO_VERTICAL_OFFSETTYPE::varchar AS ACT_TO_VERTICAL_OFFSETTYPE, 
                        src:ACT_TO_VERTICAL_OFFSETUOM::varchar AS ACT_TO_VERTICAL_OFFSETUOM, 
                        src:ACT_TO_XCOORDINATE::numeric(38, 10) AS ACT_TO_XCOORDINATE, 
                        src:ACT_TO_YCOORDINATE::numeric(38, 10) AS ACT_TO_YCOORDINATE, 
                        src:ACT_TPF::varchar AS ACT_TPF, 
                        src:ACT_TRADE::varchar AS ACT_TRADE, 
                        src:ACT_UDFCHAR01::varchar AS ACT_UDFCHAR01, 
                        src:ACT_UDFCHAR02::varchar AS ACT_UDFCHAR02, 
                        src:ACT_UDFCHAR03::varchar AS ACT_UDFCHAR03, 
                        src:ACT_UDFCHAR04::varchar AS ACT_UDFCHAR04, 
                        src:ACT_UDFCHAR05::varchar AS ACT_UDFCHAR05, 
                        src:ACT_UDFCHAR06::varchar AS ACT_UDFCHAR06, 
                        src:ACT_UDFCHAR07::varchar AS ACT_UDFCHAR07, 
                        src:ACT_UDFCHAR08::varchar AS ACT_UDFCHAR08, 
                        src:ACT_UDFCHAR09::varchar AS ACT_UDFCHAR09, 
                        src:ACT_UDFCHAR10::varchar AS ACT_UDFCHAR10, 
                        src:ACT_UDFCHAR11::varchar AS ACT_UDFCHAR11, 
                        src:ACT_UDFCHAR12::varchar AS ACT_UDFCHAR12, 
                        src:ACT_UDFCHAR13::varchar AS ACT_UDFCHAR13, 
                        src:ACT_UDFCHAR14::varchar AS ACT_UDFCHAR14, 
                        src:ACT_UDFCHAR15::varchar AS ACT_UDFCHAR15, 
                        src:ACT_UDFCHAR16::varchar AS ACT_UDFCHAR16, 
                        src:ACT_UDFCHAR17::varchar AS ACT_UDFCHAR17, 
                        src:ACT_UDFCHAR18::varchar AS ACT_UDFCHAR18, 
                        src:ACT_UDFCHAR19::varchar AS ACT_UDFCHAR19, 
                        src:ACT_UDFCHAR20::varchar AS ACT_UDFCHAR20, 
                        src:ACT_UDFCHAR21::varchar AS ACT_UDFCHAR21, 
                        src:ACT_UDFCHAR22::varchar AS ACT_UDFCHAR22, 
                        src:ACT_UDFCHAR23::varchar AS ACT_UDFCHAR23, 
                        src:ACT_UDFCHAR24::varchar AS ACT_UDFCHAR24, 
                        src:ACT_UDFCHAR25::varchar AS ACT_UDFCHAR25, 
                        src:ACT_UDFCHAR26::varchar AS ACT_UDFCHAR26, 
                        src:ACT_UDFCHAR27::varchar AS ACT_UDFCHAR27, 
                        src:ACT_UDFCHAR28::varchar AS ACT_UDFCHAR28, 
                        src:ACT_UDFCHAR29::varchar AS ACT_UDFCHAR29, 
                        src:ACT_UDFCHAR30::varchar AS ACT_UDFCHAR30, 
                        src:ACT_UDFCHKBOX01::varchar AS ACT_UDFCHKBOX01, 
                        src:ACT_UDFCHKBOX02::varchar AS ACT_UDFCHKBOX02, 
                        src:ACT_UDFCHKBOX03::varchar AS ACT_UDFCHKBOX03, 
                        src:ACT_UDFCHKBOX04::varchar AS ACT_UDFCHKBOX04, 
                        src:ACT_UDFCHKBOX05::varchar AS ACT_UDFCHKBOX05, 
                        src:ACT_UDFDATE01::datetime AS ACT_UDFDATE01, 
                        src:ACT_UDFDATE02::datetime AS ACT_UDFDATE02, 
                        src:ACT_UDFDATE03::datetime AS ACT_UDFDATE03, 
                        src:ACT_UDFDATE04::datetime AS ACT_UDFDATE04, 
                        src:ACT_UDFDATE05::datetime AS ACT_UDFDATE05, 
                        src:ACT_UDFNOTE01::varchar AS ACT_UDFNOTE01, 
                        src:ACT_UDFNOTE02::varchar AS ACT_UDFNOTE02, 
                        src:ACT_UDFNUM01::numeric(38, 10) AS ACT_UDFNUM01, 
                        src:ACT_UDFNUM02::numeric(38, 10) AS ACT_UDFNUM02, 
                        src:ACT_UDFNUM03::numeric(38, 10) AS ACT_UDFNUM03, 
                        src:ACT_UDFNUM04::numeric(38, 10) AS ACT_UDFNUM04, 
                        src:ACT_UDFNUM05::numeric(38, 10) AS ACT_UDFNUM05, 
                        src:ACT_UOM::varchar AS ACT_UOM, 
                        src:ACT_UPDATECOUNT::numeric(38, 10) AS ACT_UPDATECOUNT, 
                        src:ACT_WAP::varchar AS ACT_WAP, 
                        src:ACT_WARRANTY::varchar AS ACT_WARRANTY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ACT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ACT_ACT,
                src:ACT_EVENT  ORDER BY 
            src:ACT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTIVITIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_DEFERREDORIGACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ESTLABORCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ESTMATLCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ESTMISCCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROMPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_FROM_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_INVOICELINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_INVOICE_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_LATESTSCHED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_LEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_MATLREV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_MAXDUR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_NEWDUR), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_NEWSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_NT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_NTRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ORDLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_OT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_OTRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_PARENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_PERCOMPLETE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_PERFORMED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_PERFORMED2), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_PURRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_QTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_REQLINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_REVIEWED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_SCHEDHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_SCHEDULINGSESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_SEQ), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_START), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TASKREV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TOPOINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TOPPARENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_HORIZONTAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_LATITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_LONGITUDE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_OFFSET_PERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_REFERENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_VERTICAL_OFFSET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_TO_YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is not null