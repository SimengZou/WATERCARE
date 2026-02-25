CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TRADERATES AS SELECT
                        src:TRR_ACTIVE::varchar AS TRR_ACTIVE, 
                        src:TRR_END::datetime AS TRR_END, 
                        src:TRR_LASTSAVED::datetime AS TRR_LASTSAVED, 
                        src:TRR_MRC::varchar AS TRR_MRC, 
                        src:TRR_NTRATE::numeric(38, 10) AS TRR_NTRATE, 
                        src:TRR_OCTYPE::varchar AS TRR_OCTYPE, 
                        src:TRR_ORG::varchar AS TRR_ORG, 
                        src:TRR_OTRATE::numeric(38, 10) AS TRR_OTRATE, 
                        src:TRR_PERSON::varchar AS TRR_PERSON, 
                        src:TRR_START::datetime AS TRR_START, 
                        src:TRR_SUPPLIER::varchar AS TRR_SUPPLIER, 
                        src:TRR_SUPPLIER_ORG::varchar AS TRR_SUPPLIER_ORG, 
                        src:TRR_TAX::varchar AS TRR_TAX, 
                        src:TRR_TRADE::varchar AS TRR_TRADE, 
                        src:TRR_UDFCHAR01::varchar AS TRR_UDFCHAR01, 
                        src:TRR_UDFCHAR02::varchar AS TRR_UDFCHAR02, 
                        src:TRR_UDFCHAR03::varchar AS TRR_UDFCHAR03, 
                        src:TRR_UDFCHAR04::varchar AS TRR_UDFCHAR04, 
                        src:TRR_UDFCHAR05::varchar AS TRR_UDFCHAR05, 
                        src:TRR_UDFCHAR06::varchar AS TRR_UDFCHAR06, 
                        src:TRR_UDFCHAR07::varchar AS TRR_UDFCHAR07, 
                        src:TRR_UDFCHAR08::varchar AS TRR_UDFCHAR08, 
                        src:TRR_UDFCHAR09::varchar AS TRR_UDFCHAR09, 
                        src:TRR_UDFCHAR10::varchar AS TRR_UDFCHAR10, 
                        src:TRR_UDFCHAR11::varchar AS TRR_UDFCHAR11, 
                        src:TRR_UDFCHAR12::varchar AS TRR_UDFCHAR12, 
                        src:TRR_UDFCHAR13::varchar AS TRR_UDFCHAR13, 
                        src:TRR_UDFCHAR14::varchar AS TRR_UDFCHAR14, 
                        src:TRR_UDFCHAR15::varchar AS TRR_UDFCHAR15, 
                        src:TRR_UDFCHAR16::varchar AS TRR_UDFCHAR16, 
                        src:TRR_UDFCHAR17::varchar AS TRR_UDFCHAR17, 
                        src:TRR_UDFCHAR18::varchar AS TRR_UDFCHAR18, 
                        src:TRR_UDFCHAR19::varchar AS TRR_UDFCHAR19, 
                        src:TRR_UDFCHAR20::varchar AS TRR_UDFCHAR20, 
                        src:TRR_UDFCHAR21::varchar AS TRR_UDFCHAR21, 
                        src:TRR_UDFCHAR22::varchar AS TRR_UDFCHAR22, 
                        src:TRR_UDFCHAR23::varchar AS TRR_UDFCHAR23, 
                        src:TRR_UDFCHAR24::varchar AS TRR_UDFCHAR24, 
                        src:TRR_UDFCHAR25::varchar AS TRR_UDFCHAR25, 
                        src:TRR_UDFCHAR26::varchar AS TRR_UDFCHAR26, 
                        src:TRR_UDFCHAR27::varchar AS TRR_UDFCHAR27, 
                        src:TRR_UDFCHAR28::varchar AS TRR_UDFCHAR28, 
                        src:TRR_UDFCHAR29::varchar AS TRR_UDFCHAR29, 
                        src:TRR_UDFCHAR30::varchar AS TRR_UDFCHAR30, 
                        src:TRR_UDFCHKBOX01::varchar AS TRR_UDFCHKBOX01, 
                        src:TRR_UDFCHKBOX02::varchar AS TRR_UDFCHKBOX02, 
                        src:TRR_UDFCHKBOX03::varchar AS TRR_UDFCHKBOX03, 
                        src:TRR_UDFCHKBOX04::varchar AS TRR_UDFCHKBOX04, 
                        src:TRR_UDFCHKBOX05::varchar AS TRR_UDFCHKBOX05, 
                        src:TRR_UDFDATE01::datetime AS TRR_UDFDATE01, 
                        src:TRR_UDFDATE02::datetime AS TRR_UDFDATE02, 
                        src:TRR_UDFDATE03::datetime AS TRR_UDFDATE03, 
                        src:TRR_UDFDATE04::datetime AS TRR_UDFDATE04, 
                        src:TRR_UDFDATE05::datetime AS TRR_UDFDATE05, 
                        src:TRR_UDFNUM01::numeric(38, 10) AS TRR_UDFNUM01, 
                        src:TRR_UDFNUM02::numeric(38, 10) AS TRR_UDFNUM02, 
                        src:TRR_UDFNUM03::numeric(38, 10) AS TRR_UDFNUM03, 
                        src:TRR_UDFNUM04::numeric(38, 10) AS TRR_UDFNUM04, 
                        src:TRR_UDFNUM05::numeric(38, 10) AS TRR_UDFNUM05, 
                        src:TRR_UPDATECOUNT::numeric(38, 10) AS TRR_UPDATECOUNT, 
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
    
                        
                src:TRR_MRC,
                src:TRR_OCTYPE,
                src:TRR_ORG,
                src:TRR_PERSON,
                src:TRR_START,
                src:TRR_SUPPLIER,
                src:TRR_SUPPLIER_ORG,
                src:TRR_TRADE,
            src:TRR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRR_MRC,
                src:TRR_OCTYPE,
                src:TRR_ORG,
                src:TRR_PERSON,
                src:TRR_START,
                src:TRR_SUPPLIER,
                src:TRR_SUPPLIER_ORG,
                src:TRR_TRADE  ORDER BY 
            src:TRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TRADERATES as strm))