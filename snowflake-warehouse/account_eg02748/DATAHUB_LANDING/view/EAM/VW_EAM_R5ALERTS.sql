CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTS AS SELECT
                        src:ALT_ACTIVE::varchar AS ALT_ACTIVE, 
                        src:ALT_CODE::varchar AS ALT_CODE, 
                        src:ALT_CODEFIELDID::numeric(38, 10) AS ALT_CODEFIELDID, 
                        src:ALT_CREATED::datetime AS ALT_CREATED, 
                        src:ALT_CREATEDBY::varchar AS ALT_CREATEDBY, 
                        src:ALT_DATASPYID::numeric(38, 10) AS ALT_DATASPYID, 
                        src:ALT_DESC::varchar AS ALT_DESC, 
                        src:ALT_ENABLEGENERATEWO::varchar AS ALT_ENABLEGENERATEWO, 
                        src:ALT_ENABLEIONPULSE::varchar AS ALT_ENABLEIONPULSE, 
                        src:ALT_ENABLEMAIL::varchar AS ALT_ENABLEMAIL, 
                        src:ALT_ENABLEWO::varchar AS ALT_ENABLEWO, 
                        src:ALT_ENABLEWOPICKTICKET::varchar AS ALT_ENABLEWOPICKTICKET, 
                        src:ALT_ENTITY::varchar AS ALT_ENTITY, 
                        src:ALT_FREQ::numeric(38, 10) AS ALT_FREQ, 
                        src:ALT_FREQUOM::varchar AS ALT_FREQUOM, 
                        src:ALT_GRIDID::numeric(38, 10) AS ALT_GRIDID, 
                        src:ALT_IMPORT_REF::varchar AS ALT_IMPORT_REF, 
                        src:ALT_INPROGRESS::varchar AS ALT_INPROGRESS, 
                        src:ALT_LASTALERT::datetime AS ALT_LASTALERT, 
                        src:ALT_LASTEVAL::datetime AS ALT_LASTEVAL, 
                        src:ALT_LASTSAVED::datetime AS ALT_LASTSAVED, 
                        src:ALT_MAXVALUE::numeric(38, 10) AS ALT_MAXVALUE, 
                        src:ALT_MINVALUE::numeric(38, 10) AS ALT_MINVALUE, 
                        src:ALT_NEXTEVAL::datetime AS ALT_NEXTEVAL, 
                        src:ALT_ORGFIELDID::numeric(38, 10) AS ALT_ORGFIELDID, 
                        src:ALT_RENTITY::varchar AS ALT_RENTITY, 
                        src:ALT_SOURCE::varchar AS ALT_SOURCE, 
                        src:ALT_UDFCHAR01::varchar AS ALT_UDFCHAR01, 
                        src:ALT_UDFCHAR02::varchar AS ALT_UDFCHAR02, 
                        src:ALT_UDFCHAR03::varchar AS ALT_UDFCHAR03, 
                        src:ALT_UDFCHAR04::varchar AS ALT_UDFCHAR04, 
                        src:ALT_UDFCHAR05::varchar AS ALT_UDFCHAR05, 
                        src:ALT_UDFCHAR06::varchar AS ALT_UDFCHAR06, 
                        src:ALT_UDFCHAR07::varchar AS ALT_UDFCHAR07, 
                        src:ALT_UDFCHAR08::varchar AS ALT_UDFCHAR08, 
                        src:ALT_UDFCHAR09::varchar AS ALT_UDFCHAR09, 
                        src:ALT_UDFCHAR10::varchar AS ALT_UDFCHAR10, 
                        src:ALT_UDFCHKBOX01::varchar AS ALT_UDFCHKBOX01, 
                        src:ALT_UDFCHKBOX02::varchar AS ALT_UDFCHKBOX02, 
                        src:ALT_UDFCHKBOX03::varchar AS ALT_UDFCHKBOX03, 
                        src:ALT_UDFCHKBOX04::varchar AS ALT_UDFCHKBOX04, 
                        src:ALT_UDFCHKBOX05::varchar AS ALT_UDFCHKBOX05, 
                        src:ALT_UDFDATE01::datetime AS ALT_UDFDATE01, 
                        src:ALT_UDFDATE02::datetime AS ALT_UDFDATE02, 
                        src:ALT_UDFDATE03::datetime AS ALT_UDFDATE03, 
                        src:ALT_UDFDATE04::datetime AS ALT_UDFDATE04, 
                        src:ALT_UDFDATE05::datetime AS ALT_UDFDATE05, 
                        src:ALT_UDFNUM01::numeric(38, 10) AS ALT_UDFNUM01, 
                        src:ALT_UDFNUM02::numeric(38, 10) AS ALT_UDFNUM02, 
                        src:ALT_UDFNUM03::numeric(38, 10) AS ALT_UDFNUM03, 
                        src:ALT_UDFNUM04::numeric(38, 10) AS ALT_UDFNUM04, 
                        src:ALT_UDFNUM05::numeric(38, 10) AS ALT_UDFNUM05, 
                        src:ALT_UDFNUM06::numeric(38, 10) AS ALT_UDFNUM06, 
                        src:ALT_UDFNUM07::numeric(38, 10) AS ALT_UDFNUM07, 
                        src:ALT_UDFNUM08::numeric(38, 10) AS ALT_UDFNUM08, 
                        src:ALT_UDFNUM09::numeric(38, 10) AS ALT_UDFNUM09, 
                        src:ALT_UDFNUM10::numeric(38, 10) AS ALT_UDFNUM10, 
                        src:ALT_UPDATECOUNT::numeric(38, 10) AS ALT_UPDATECOUNT, 
                        src:ALT_USEMINMAX::varchar AS ALT_USEMINMAX, 
                        src:ALT_VALUEFIELDID::numeric(38, 10) AS ALT_VALUEFIELDID, 
                        src:ALT_WITHINVALUES::varchar AS ALT_WITHINVALUES, 
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
    
                        
                src:ALT_CODE,
            src:ALT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ALT_CODE  ORDER BY 
            src:ALT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTS as strm))