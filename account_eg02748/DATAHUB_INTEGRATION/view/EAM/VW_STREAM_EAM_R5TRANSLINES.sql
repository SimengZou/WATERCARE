CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRANSLINES AS SELECT
                        src:TRL_ACD::numeric(38, 10) AS TRL_ACD, 
                        src:TRL_ACT::numeric(38, 10) AS TRL_ACT, 
                        src:TRL_ALLOCATE::varchar AS TRL_ALLOCATE, 
                        src:TRL_ATTACH::varchar AS TRL_ATTACH, 
                        src:TRL_ATTACHTO::varchar AS TRL_ATTACHTO, 
                        src:TRL_ATTACHTO_ORG::varchar AS TRL_ATTACHTO_ORG, 
                        src:TRL_AVGPRICE::numeric(38, 10) AS TRL_AVGPRICE, 
                        src:TRL_BILLSUBLINE::varchar AS TRL_BILLSUBLINE, 
                        src:TRL_BIN::varchar AS TRL_BIN, 
                        src:TRL_CALOTHERCOST::numeric(38, 10) AS TRL_CALOTHERCOST, 
                        src:TRL_CONSIGNMENT::varchar AS TRL_CONSIGNMENT, 
                        src:TRL_CONSIGNSUPPLIER::varchar AS TRL_CONSIGNSUPPLIER, 
                        src:TRL_CONSIGNSUPPLIER_ORG::varchar AS TRL_CONSIGNSUPPLIER_ORG, 
                        src:TRL_CORERETURNPRINTQTY::numeric(38, 10) AS TRL_CORERETURNPRINTQTY, 
                        src:TRL_COSTCODE::varchar AS TRL_COSTCODE, 
                        src:TRL_CREATED::datetime AS TRL_CREATED, 
                        src:TRL_DATE::datetime AS TRL_DATE, 
                        src:TRL_DCKCODE::varchar AS TRL_DCKCODE, 
                        src:TRL_DCKLINE::numeric(38, 10) AS TRL_DCKLINE, 
                        src:TRL_DESC::varchar AS TRL_DESC, 
                        src:TRL_EVENT::varchar AS TRL_EVENT, 
                        src:TRL_EVTALIAS::varchar AS TRL_EVTALIAS, 
                        src:TRL_FLEETCHECKED::varchar AS TRL_FLEETCHECKED, 
                        src:TRL_FLEETMARKUP::numeric(38, 10) AS TRL_FLEETMARKUP, 
                        src:TRL_GLTRANSFER::datetime AS TRL_GLTRANSFER, 
                        src:TRL_GLTRANSFERFLAG::varchar AS TRL_GLTRANSFERFLAG, 
                        src:TRL_HELDITEM::varchar AS TRL_HELDITEM, 
                        src:TRL_INTERFACE::datetime AS TRL_INTERFACE, 
                        src:TRL_INVOICE::varchar AS TRL_INVOICE, 
                        src:TRL_INVOICELINE::numeric(38, 10) AS TRL_INVOICELINE, 
                        src:TRL_INVOICE_ORG::varchar AS TRL_INVOICE_ORG, 
                        src:TRL_INVOICE_REVISION::numeric(38, 10) AS TRL_INVOICE_REVISION, 
                        src:TRL_IO::numeric(38, 10) AS TRL_IO, 
                        src:TRL_ISSUEDPART::varchar AS TRL_ISSUEDPART, 
                        src:TRL_JECATEGORY::varchar AS TRL_JECATEGORY, 
                        src:TRL_JESOURCE::varchar AS TRL_JESOURCE, 
                        src:TRL_LASTSAVED::datetime AS TRL_LASTSAVED, 
                        src:TRL_LEAK::varchar AS TRL_LEAK, 
                        src:TRL_LINE::numeric(38, 10) AS TRL_LINE, 
                        src:TRL_LOT::varchar AS TRL_LOT, 
                        src:TRL_MANUFACTPART::varchar AS TRL_MANUFACTPART, 
                        src:TRL_MANUFACTURER::varchar AS TRL_MANUFACTURER, 
                        src:TRL_MULTIPLY::numeric(38, 10) AS TRL_MULTIPLY, 
                        src:TRL_NEWCOSTCODE::varchar AS TRL_NEWCOSTCODE, 
                        src:TRL_NEWMANUFACT::varchar AS TRL_NEWMANUFACT, 
                        src:TRL_NEWMRC::varchar AS TRL_NEWMRC, 
                        src:TRL_OBJECT::varchar AS TRL_OBJECT, 
                        src:TRL_OBJECT_ORG::varchar AS TRL_OBJECT_ORG, 
                        src:TRL_OBRTYPE::varchar AS TRL_OBRTYPE, 
                        src:TRL_OBTYPE::varchar AS TRL_OBTYPE, 
                        src:TRL_ORDER::varchar AS TRL_ORDER, 
                        src:TRL_ORDER_ORG::varchar AS TRL_ORDER_ORG, 
                        src:TRL_ORDLINE::numeric(38, 10) AS TRL_ORDLINE, 
                        src:TRL_ORIGQTY::numeric(38, 10) AS TRL_ORIGQTY, 
                        src:TRL_PACKAGE::varchar AS TRL_PACKAGE, 
                        src:TRL_PART::varchar AS TRL_PART, 
                        src:TRL_PARTLOCATION::varchar AS TRL_PARTLOCATION, 
                        src:TRL_PART_ORG::varchar AS TRL_PART_ORG, 
                        src:TRL_PICKLIST::varchar AS TRL_PICKLIST, 
                        src:TRL_POEXTRACHARGES::numeric(38, 10) AS TRL_POEXTRACHARGES, 
                        src:TRL_POTAXAMOUNT::numeric(38, 10) AS TRL_POTAXAMOUNT, 
                        src:TRL_PRICE::numeric(38, 10) AS TRL_PRICE, 
                        src:TRL_PRICEUPDATE::varchar AS TRL_PRICEUPDATE, 
                        src:TRL_PRINTQTY::numeric(38, 10) AS TRL_PRINTQTY, 
                        src:TRL_PROJBUD::varchar AS TRL_PROJBUD, 
                        src:TRL_PROJECT::varchar AS TRL_PROJECT, 
                        src:TRL_QTY::numeric(38, 10) AS TRL_QTY, 
                        src:TRL_REJECTREASON::varchar AS TRL_REJECTREASON, 
                        src:TRL_REPAIRPRICE::numeric(38, 10) AS TRL_REPAIRPRICE, 
                        src:TRL_REQ::varchar AS TRL_REQ, 
                        src:TRL_REQLINE::numeric(38, 10) AS TRL_REQLINE, 
                        src:TRL_ROUTEREC::varchar AS TRL_ROUTEREC, 
                        src:TRL_RTYPE::varchar AS TRL_RTYPE, 
                        src:TRL_SCRAPOBJECT::varchar AS TRL_SCRAPOBJECT, 
                        src:TRL_SCRAPOBJECT_ORG::varchar AS TRL_SCRAPOBJECT_ORG, 
                        src:TRL_SCRAPQTY::numeric(38, 10) AS TRL_SCRAPQTY, 
                        src:TRL_SOURCECODE::varchar AS TRL_SOURCECODE, 
                        src:TRL_SOURCESYSTEM::varchar AS TRL_SOURCESYSTEM, 
                        src:TRL_SPLITTRANS::varchar AS TRL_SPLITTRANS, 
                        src:TRL_STOCKPRICE::numeric(38, 10) AS TRL_STOCKPRICE, 
                        src:TRL_STORE::varchar AS TRL_STORE, 
                        src:TRL_SUPPLIER::varchar AS TRL_SUPPLIER, 
                        src:TRL_SUPPLIER_ORG::varchar AS TRL_SUPPLIER_ORG, 
                        src:TRL_TAXJURISDICTION::varchar AS TRL_TAXJURISDICTION, 
                        src:TRL_TRANS::varchar AS TRL_TRANS, 
                        src:TRL_TRANSCODE::varchar AS TRL_TRANSCODE, 
                        src:TRL_TRANSGROUP::numeric(38, 10) AS TRL_TRANSGROUP, 
                        src:TRL_TRANSORGID::numeric(38, 10) AS TRL_TRANSORGID, 
                        src:TRL_TYPE::varchar AS TRL_TYPE, 
                        src:TRL_UDFCHAR01::varchar AS TRL_UDFCHAR01, 
                        src:TRL_UDFCHAR02::varchar AS TRL_UDFCHAR02, 
                        src:TRL_UDFCHAR03::varchar AS TRL_UDFCHAR03, 
                        src:TRL_UDFCHAR04::varchar AS TRL_UDFCHAR04, 
                        src:TRL_UDFCHAR05::varchar AS TRL_UDFCHAR05, 
                        src:TRL_UDFCHAR06::varchar AS TRL_UDFCHAR06, 
                        src:TRL_UDFCHAR07::varchar AS TRL_UDFCHAR07, 
                        src:TRL_UDFCHAR08::varchar AS TRL_UDFCHAR08, 
                        src:TRL_UDFCHAR09::varchar AS TRL_UDFCHAR09, 
                        src:TRL_UDFCHAR10::varchar AS TRL_UDFCHAR10, 
                        src:TRL_UDFCHAR11::varchar AS TRL_UDFCHAR11, 
                        src:TRL_UDFCHAR12::varchar AS TRL_UDFCHAR12, 
                        src:TRL_UDFCHAR13::varchar AS TRL_UDFCHAR13, 
                        src:TRL_UDFCHAR14::varchar AS TRL_UDFCHAR14, 
                        src:TRL_UDFCHAR15::varchar AS TRL_UDFCHAR15, 
                        src:TRL_UDFCHAR16::varchar AS TRL_UDFCHAR16, 
                        src:TRL_UDFCHAR17::varchar AS TRL_UDFCHAR17, 
                        src:TRL_UDFCHAR18::varchar AS TRL_UDFCHAR18, 
                        src:TRL_UDFCHAR19::varchar AS TRL_UDFCHAR19, 
                        src:TRL_UDFCHAR20::varchar AS TRL_UDFCHAR20, 
                        src:TRL_UDFCHAR21::varchar AS TRL_UDFCHAR21, 
                        src:TRL_UDFCHAR22::varchar AS TRL_UDFCHAR22, 
                        src:TRL_UDFCHAR23::varchar AS TRL_UDFCHAR23, 
                        src:TRL_UDFCHAR24::varchar AS TRL_UDFCHAR24, 
                        src:TRL_UDFCHAR25::varchar AS TRL_UDFCHAR25, 
                        src:TRL_UDFCHAR26::varchar AS TRL_UDFCHAR26, 
                        src:TRL_UDFCHAR27::varchar AS TRL_UDFCHAR27, 
                        src:TRL_UDFCHAR28::varchar AS TRL_UDFCHAR28, 
                        src:TRL_UDFCHAR29::varchar AS TRL_UDFCHAR29, 
                        src:TRL_UDFCHAR30::varchar AS TRL_UDFCHAR30, 
                        src:TRL_UDFCHKBOX01::varchar AS TRL_UDFCHKBOX01, 
                        src:TRL_UDFCHKBOX02::varchar AS TRL_UDFCHKBOX02, 
                        src:TRL_UDFCHKBOX03::varchar AS TRL_UDFCHKBOX03, 
                        src:TRL_UDFCHKBOX04::varchar AS TRL_UDFCHKBOX04, 
                        src:TRL_UDFCHKBOX05::varchar AS TRL_UDFCHKBOX05, 
                        src:TRL_UDFDATE01::datetime AS TRL_UDFDATE01, 
                        src:TRL_UDFDATE02::datetime AS TRL_UDFDATE02, 
                        src:TRL_UDFDATE03::datetime AS TRL_UDFDATE03, 
                        src:TRL_UDFDATE04::datetime AS TRL_UDFDATE04, 
                        src:TRL_UDFDATE05::datetime AS TRL_UDFDATE05, 
                        src:TRL_UDFNUM01::numeric(38, 10) AS TRL_UDFNUM01, 
                        src:TRL_UDFNUM02::numeric(38, 10) AS TRL_UDFNUM02, 
                        src:TRL_UDFNUM03::numeric(38, 10) AS TRL_UDFNUM03, 
                        src:TRL_UDFNUM04::numeric(38, 10) AS TRL_UDFNUM04, 
                        src:TRL_UDFNUM05::numeric(38, 10) AS TRL_UDFNUM05, 
                        src:TRL_UPDATECOUNT::numeric(38, 10) AS TRL_UPDATECOUNT, 
                        src:TRL_UPDATED::datetime AS TRL_UPDATED, 
                        src:TRL_WARRANTY::varchar AS TRL_WARRANTY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TRL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TRL_LINE,
                src:TRL_TRANS  ORDER BY 
            src:TRL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRANSLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_ACD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_ACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_AVGPRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_CALOTHERCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_CORERETURNPRINTQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_DCKLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_FLEETMARKUP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_GLTRANSFER), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_INTERFACE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_INVOICELINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_INVOICE_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_IO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_MULTIPLY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_ORDLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_ORIGQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_POEXTRACHARGES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_POTAXAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_PRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_PRINTQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_QTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_REPAIRPRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_REQLINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_SCRAPQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_STOCKPRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_TRANSGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_TRANSORGID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRL_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRL_LASTSAVED), '1900-01-01')) is not null