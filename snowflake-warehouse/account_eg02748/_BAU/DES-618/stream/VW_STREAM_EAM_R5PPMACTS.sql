CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PPMACTS
            AS
            SELECT
                src:"PPA_ACT"::numeric(38, 10) AS PPA_ACT, 
                src:"PPA_ACTION"::varchar AS PPA_ACTION, 
                src:"PPA_ASMLEVEL"::varchar AS PPA_ASMLEVEL, 
                src:"PPA_COMPLEVEL"::varchar AS PPA_COMPLEVEL, 
                src:"PPA_CONDITION"::varchar AS PPA_CONDITION, 
                src:"PPA_DURATION"::numeric(38, 10) AS PPA_DURATION, 
                src:"PPA_EST"::numeric(38, 10) AS PPA_EST, 
                src:"PPA_HIRE"::varchar AS PPA_HIRE, 
                src:"PPA_HOW"::varchar AS PPA_HOW, 
                src:"PPA_LABORRTYPE"::varchar AS PPA_LABORRTYPE, 
                src:"PPA_LABORTYPE"::varchar AS PPA_LABORTYPE, 
                src:"PPA_LASTSAVED"::datetime AS PPA_LASTSAVED,  
                src:"PPA_LASTSAVED"::datetime as ETL_LASTSAVED,
                src:"PPA_MANUFACT"::varchar AS PPA_MANUFACT, 
                src:"PPA_MATLIST"::varchar AS PPA_MATLIST, 
                src:"PPA_NOTE"::varchar AS PPA_NOTE, 
                src:"PPA_PERSONS"::numeric(38, 10) AS PPA_PERSONS, 
                src:"PPA_PPM"::varchar AS PPA_PPM, 
                src:"PPA_QTY"::numeric(38, 10) AS PPA_QTY, 
                src:"PPA_REVISION"::numeric(38, 10) AS PPA_REVISION, 
                src:"PPA_RPC"::varchar AS PPA_RPC, 
                src:"PPA_SPECIAL"::varchar AS PPA_SPECIAL, 
                src:"PPA_START"::numeric(38, 10) AS PPA_START, 
                src:"PPA_SUPPLIER"::varchar AS PPA_SUPPLIER, 
                src:"PPA_SUPPLIER_ORG"::varchar AS PPA_SUPPLIER_ORG, 
                src:"PPA_SYSLEVEL"::varchar AS PPA_SYSLEVEL, 
                src:"PPA_TASK"::varchar AS PPA_TASK, 
                src:"PPA_TPF"::varchar AS PPA_TPF, 
                src:"PPA_TRADE"::varchar AS PPA_TRADE, 
                src:"PPA_UDFCHAR01"::varchar AS PPA_UDFCHAR01, 
                src:"PPA_UDFCHAR02"::varchar AS PPA_UDFCHAR02, 
                src:"PPA_UDFCHAR03"::varchar AS PPA_UDFCHAR03, 
                src:"PPA_UDFCHAR04"::varchar AS PPA_UDFCHAR04, 
                src:"PPA_UDFCHAR05"::varchar AS PPA_UDFCHAR05, 
                src:"PPA_UDFCHAR06"::varchar AS PPA_UDFCHAR06, 
                src:"PPA_UDFCHAR07"::varchar AS PPA_UDFCHAR07, 
                src:"PPA_UDFCHAR08"::varchar AS PPA_UDFCHAR08, 
                src:"PPA_UDFCHAR09"::varchar AS PPA_UDFCHAR09, 
                src:"PPA_UDFCHAR10"::varchar AS PPA_UDFCHAR10, 
                src:"PPA_UDFCHAR11"::varchar AS PPA_UDFCHAR11, 
                src:"PPA_UDFCHAR12"::varchar AS PPA_UDFCHAR12, 
                src:"PPA_UDFCHAR13"::varchar AS PPA_UDFCHAR13, 
                src:"PPA_UDFCHAR14"::varchar AS PPA_UDFCHAR14, 
                src:"PPA_UDFCHAR15"::varchar AS PPA_UDFCHAR15, 
                src:"PPA_UDFCHAR16"::varchar AS PPA_UDFCHAR16, 
                src:"PPA_UDFCHAR17"::varchar AS PPA_UDFCHAR17, 
                src:"PPA_UDFCHAR18"::varchar AS PPA_UDFCHAR18, 
                src:"PPA_UDFCHAR19"::varchar AS PPA_UDFCHAR19, 
                src:"PPA_UDFCHAR20"::varchar AS PPA_UDFCHAR20, 
                src:"PPA_UDFCHAR21"::varchar AS PPA_UDFCHAR21, 
                src:"PPA_UDFCHAR22"::varchar AS PPA_UDFCHAR22, 
                src:"PPA_UDFCHAR23"::varchar AS PPA_UDFCHAR23, 
                src:"PPA_UDFCHAR24"::varchar AS PPA_UDFCHAR24, 
                src:"PPA_UDFCHAR25"::varchar AS PPA_UDFCHAR25, 
                src:"PPA_UDFCHAR26"::varchar AS PPA_UDFCHAR26, 
                src:"PPA_UDFCHAR27"::varchar AS PPA_UDFCHAR27, 
                src:"PPA_UDFCHAR28"::varchar AS PPA_UDFCHAR28, 
                src:"PPA_UDFCHAR29"::varchar AS PPA_UDFCHAR29, 
                src:"PPA_UDFCHAR30"::varchar AS PPA_UDFCHAR30, 
                src:"PPA_UDFCHKBOX01"::varchar AS PPA_UDFCHKBOX01, 
                src:"PPA_UDFCHKBOX02"::varchar AS PPA_UDFCHKBOX02, 
                src:"PPA_UDFCHKBOX03"::varchar AS PPA_UDFCHKBOX03, 
                src:"PPA_UDFCHKBOX04"::varchar AS PPA_UDFCHKBOX04, 
                src:"PPA_UDFCHKBOX05"::varchar AS PPA_UDFCHKBOX05, 
                src:"PPA_UDFDATE01"::datetime AS PPA_UDFDATE01, 
                src:"PPA_UDFDATE02"::datetime AS PPA_UDFDATE02, 
                src:"PPA_UDFDATE03"::datetime AS PPA_UDFDATE03, 
                src:"PPA_UDFDATE04"::datetime AS PPA_UDFDATE04, 
                src:"PPA_UDFDATE05"::datetime AS PPA_UDFDATE05, 
                src:"PPA_UDFNOTE01"::varchar AS PPA_UDFNOTE01, 
                src:"PPA_UDFNOTE02"::varchar AS PPA_UDFNOTE02, 
                src:"PPA_UDFNUM01"::numeric(38, 10) AS PPA_UDFNUM01, 
                src:"PPA_UDFNUM02"::numeric(38, 10) AS PPA_UDFNUM02, 
                src:"PPA_UDFNUM03"::numeric(38, 10) AS PPA_UDFNUM03, 
                src:"PPA_UDFNUM04"::numeric(38, 10) AS PPA_UDFNUM04, 
                src:"PPA_UDFNUM05"::numeric(38, 10) AS PPA_UDFNUM05, 
                src:"PPA_UOM"::varchar AS PPA_UOM, 
                src:"PPA_UPDATECOUNT"::numeric(38, 10) AS PPA_UPDATECOUNT, 
                src:"PPA_WAP"::varchar AS PPA_WAP, 
                src:"PPA_WHAT"::varchar AS PPA_WHAT, 
                src:"PPA_WHERE"::varchar AS PPA_WHERE, 
                src:"_DELETED"::boolean AS _DELETED, 
                src:"_DELETED"::BOOLEAN as ETL_DELETED, 
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
     
                            
                src:"PPA_ACT"::numeric(38, 10),
                src:"PPA_PPM"::varchar,
                src:"PPA_REVISION"::numeric(38, 10),
                src:"PPA_LASTSAVED"::datetime
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:"PPA_ACT"::numeric(38, 10),
                src:"PPA_PPM"::varchar,
                src:"PPA_REVISION"::numeric(38, 10)  ORDER BY 
                src:"PPA_LASTSAVED"::datetime desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMACTS as strm

)
    WHERE
    ROWNUMBER=1)
                ; 