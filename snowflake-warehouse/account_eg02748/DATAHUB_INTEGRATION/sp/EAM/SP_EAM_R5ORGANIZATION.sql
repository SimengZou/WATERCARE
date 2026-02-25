create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ORGANIZATION()
    returns varchar not null
                language javascript
                as
                $$

//  Variables

var result = "";                                    // return status of this proc call
const process_name = Object.keys(this)[0];          // name of currently executing process
var number_of_rows_inserted = 0;                             // track number of rows we have inserted
var number_of_rows_updated = 0;                             // track number of rows we have updated


//  Step 1.

//  Start execution - log start

log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', 0,0,0);";
snowflake.execute({sqlText: log_sql_command});

snowflake.execute( {sqlText: "begin transaction;"} );
try
    {
        var sql_command = `
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ORGANIZATION_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ORGANIZATION as target using (SELECT * FROM (SELECT 
            strm.ORG_ACCOUNTINGENTITY, 
            strm.ORG_BOOKID, 
            strm.ORG_CALGROUPCODE, 
            strm.ORG_CALGROUPORG, 
            strm.ORG_CODE, 
            strm.ORG_CODEREF, 
            strm.ORG_COMMON, 
            strm.ORG_CREATED, 
            strm.ORG_CURR, 
            strm.ORG_DEPMETHOD, 
            strm.ORG_DEPRMETHOD, 
            strm.ORG_DESC, 
            strm.ORG_DUNSNUM, 
            strm.ORG_EXTERNALORG, 
            strm.ORG_INVQTYTOL, 
            strm.ORG_LASTSAVED, 
            strm.ORG_LATITUDE, 
            strm.ORG_LOCALE, 
            strm.ORG_LONGITUDE, 
            strm.ORG_MATCHLTA, 
            strm.ORG_MATCHLTP, 
            strm.ORG_REQUESTRECALCDEP, 
            strm.ORG_SEGMENTVALUE, 
            strm.ORG_STREAMPLUSPROJECT, 
            strm.ORG_TIMEZONE, 
            strm.ORG_UDFCHAR01, 
            strm.ORG_UDFCHAR02, 
            strm.ORG_UDFCHAR03, 
            strm.ORG_UDFCHAR04, 
            strm.ORG_UDFCHAR05, 
            strm.ORG_UDFCHAR06, 
            strm.ORG_UDFCHAR07, 
            strm.ORG_UDFCHAR08, 
            strm.ORG_UDFCHAR09, 
            strm.ORG_UDFCHAR10, 
            strm.ORG_UDFCHKBOX01, 
            strm.ORG_UDFCHKBOX02, 
            strm.ORG_UDFCHKBOX03, 
            strm.ORG_UDFCHKBOX04, 
            strm.ORG_UDFCHKBOX05, 
            strm.ORG_UDFDATE01, 
            strm.ORG_UDFDATE02, 
            strm.ORG_UDFDATE03, 
            strm.ORG_UDFDATE04, 
            strm.ORG_UDFDATE05, 
            strm.ORG_UDFNUM01, 
            strm.ORG_UDFNUM02, 
            strm.ORG_UDFNUM03, 
            strm.ORG_UDFNUM04, 
            strm.ORG_UDFNUM05, 
            strm.ORG_UPDATECOUNT, 
            strm.ORG_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ORG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ORGANIZATION as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ORG_CODE=src.ORG_CODE) OR (target.ORG_CODE IS NULL AND src.ORG_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ORG_ACCOUNTINGENTITY=src.ORG_ACCOUNTINGENTITY, 
                    target.ORG_BOOKID=src.ORG_BOOKID, 
                    target.ORG_CALGROUPCODE=src.ORG_CALGROUPCODE, 
                    target.ORG_CALGROUPORG=src.ORG_CALGROUPORG, 
                    target.ORG_CODE=src.ORG_CODE, 
                    target.ORG_CODEREF=src.ORG_CODEREF, 
                    target.ORG_COMMON=src.ORG_COMMON, 
                    target.ORG_CREATED=src.ORG_CREATED, 
                    target.ORG_CURR=src.ORG_CURR, 
                    target.ORG_DEPMETHOD=src.ORG_DEPMETHOD, 
                    target.ORG_DEPRMETHOD=src.ORG_DEPRMETHOD, 
                    target.ORG_DESC=src.ORG_DESC, 
                    target.ORG_DUNSNUM=src.ORG_DUNSNUM, 
                    target.ORG_EXTERNALORG=src.ORG_EXTERNALORG, 
                    target.ORG_INVQTYTOL=src.ORG_INVQTYTOL, 
                    target.ORG_LASTSAVED=src.ORG_LASTSAVED, 
                    target.ORG_LATITUDE=src.ORG_LATITUDE, 
                    target.ORG_LOCALE=src.ORG_LOCALE, 
                    target.ORG_LONGITUDE=src.ORG_LONGITUDE, 
                    target.ORG_MATCHLTA=src.ORG_MATCHLTA, 
                    target.ORG_MATCHLTP=src.ORG_MATCHLTP, 
                    target.ORG_REQUESTRECALCDEP=src.ORG_REQUESTRECALCDEP, 
                    target.ORG_SEGMENTVALUE=src.ORG_SEGMENTVALUE, 
                    target.ORG_STREAMPLUSPROJECT=src.ORG_STREAMPLUSPROJECT, 
                    target.ORG_TIMEZONE=src.ORG_TIMEZONE, 
                    target.ORG_UDFCHAR01=src.ORG_UDFCHAR01, 
                    target.ORG_UDFCHAR02=src.ORG_UDFCHAR02, 
                    target.ORG_UDFCHAR03=src.ORG_UDFCHAR03, 
                    target.ORG_UDFCHAR04=src.ORG_UDFCHAR04, 
                    target.ORG_UDFCHAR05=src.ORG_UDFCHAR05, 
                    target.ORG_UDFCHAR06=src.ORG_UDFCHAR06, 
                    target.ORG_UDFCHAR07=src.ORG_UDFCHAR07, 
                    target.ORG_UDFCHAR08=src.ORG_UDFCHAR08, 
                    target.ORG_UDFCHAR09=src.ORG_UDFCHAR09, 
                    target.ORG_UDFCHAR10=src.ORG_UDFCHAR10, 
                    target.ORG_UDFCHKBOX01=src.ORG_UDFCHKBOX01, 
                    target.ORG_UDFCHKBOX02=src.ORG_UDFCHKBOX02, 
                    target.ORG_UDFCHKBOX03=src.ORG_UDFCHKBOX03, 
                    target.ORG_UDFCHKBOX04=src.ORG_UDFCHKBOX04, 
                    target.ORG_UDFCHKBOX05=src.ORG_UDFCHKBOX05, 
                    target.ORG_UDFDATE01=src.ORG_UDFDATE01, 
                    target.ORG_UDFDATE02=src.ORG_UDFDATE02, 
                    target.ORG_UDFDATE03=src.ORG_UDFDATE03, 
                    target.ORG_UDFDATE04=src.ORG_UDFDATE04, 
                    target.ORG_UDFDATE05=src.ORG_UDFDATE05, 
                    target.ORG_UDFNUM01=src.ORG_UDFNUM01, 
                    target.ORG_UDFNUM02=src.ORG_UDFNUM02, 
                    target.ORG_UDFNUM03=src.ORG_UDFNUM03, 
                    target.ORG_UDFNUM04=src.ORG_UDFNUM04, 
                    target.ORG_UDFNUM05=src.ORG_UDFNUM05, 
                    target.ORG_UPDATECOUNT=src.ORG_UPDATECOUNT, 
                    target.ORG_UPDATED=src.ORG_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ORG_ACCOUNTINGENTITY, 
                    ORG_BOOKID, 
                    ORG_CALGROUPCODE, 
                    ORG_CALGROUPORG, 
                    ORG_CODE, 
                    ORG_CODEREF, 
                    ORG_COMMON, 
                    ORG_CREATED, 
                    ORG_CURR, 
                    ORG_DEPMETHOD, 
                    ORG_DEPRMETHOD, 
                    ORG_DESC, 
                    ORG_DUNSNUM, 
                    ORG_EXTERNALORG, 
                    ORG_INVQTYTOL, 
                    ORG_LASTSAVED, 
                    ORG_LATITUDE, 
                    ORG_LOCALE, 
                    ORG_LONGITUDE, 
                    ORG_MATCHLTA, 
                    ORG_MATCHLTP, 
                    ORG_REQUESTRECALCDEP, 
                    ORG_SEGMENTVALUE, 
                    ORG_STREAMPLUSPROJECT, 
                    ORG_TIMEZONE, 
                    ORG_UDFCHAR01, 
                    ORG_UDFCHAR02, 
                    ORG_UDFCHAR03, 
                    ORG_UDFCHAR04, 
                    ORG_UDFCHAR05, 
                    ORG_UDFCHAR06, 
                    ORG_UDFCHAR07, 
                    ORG_UDFCHAR08, 
                    ORG_UDFCHAR09, 
                    ORG_UDFCHAR10, 
                    ORG_UDFCHKBOX01, 
                    ORG_UDFCHKBOX02, 
                    ORG_UDFCHKBOX03, 
                    ORG_UDFCHKBOX04, 
                    ORG_UDFCHKBOX05, 
                    ORG_UDFDATE01, 
                    ORG_UDFDATE02, 
                    ORG_UDFDATE03, 
                    ORG_UDFDATE04, 
                    ORG_UDFDATE05, 
                    ORG_UDFNUM01, 
                    ORG_UDFNUM02, 
                    ORG_UDFNUM03, 
                    ORG_UDFNUM04, 
                    ORG_UDFNUM05, 
                    ORG_UPDATECOUNT, 
                    ORG_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ORG_ACCOUNTINGENTITY, 
                    src.ORG_BOOKID, 
                    src.ORG_CALGROUPCODE, 
                    src.ORG_CALGROUPORG, 
                    src.ORG_CODE, 
                    src.ORG_CODEREF, 
                    src.ORG_COMMON, 
                    src.ORG_CREATED, 
                    src.ORG_CURR, 
                    src.ORG_DEPMETHOD, 
                    src.ORG_DEPRMETHOD, 
                    src.ORG_DESC, 
                    src.ORG_DUNSNUM, 
                    src.ORG_EXTERNALORG, 
                    src.ORG_INVQTYTOL, 
                    src.ORG_LASTSAVED, 
                    src.ORG_LATITUDE, 
                    src.ORG_LOCALE, 
                    src.ORG_LONGITUDE, 
                    src.ORG_MATCHLTA, 
                    src.ORG_MATCHLTP, 
                    src.ORG_REQUESTRECALCDEP, 
                    src.ORG_SEGMENTVALUE, 
                    src.ORG_STREAMPLUSPROJECT, 
                    src.ORG_TIMEZONE, 
                    src.ORG_UDFCHAR01, 
                    src.ORG_UDFCHAR02, 
                    src.ORG_UDFCHAR03, 
                    src.ORG_UDFCHAR04, 
                    src.ORG_UDFCHAR05, 
                    src.ORG_UDFCHAR06, 
                    src.ORG_UDFCHAR07, 
                    src.ORG_UDFCHAR08, 
                    src.ORG_UDFCHAR09, 
                    src.ORG_UDFCHAR10, 
                    src.ORG_UDFCHKBOX01, 
                    src.ORG_UDFCHKBOX02, 
                    src.ORG_UDFCHKBOX03, 
                    src.ORG_UDFCHKBOX04, 
                    src.ORG_UDFCHKBOX05, 
                    src.ORG_UDFDATE01, 
                    src.ORG_UDFDATE02, 
                    src.ORG_UDFDATE03, 
                    src.ORG_UDFDATE04, 
                    src.ORG_UDFDATE05, 
                    src.ORG_UDFNUM01, 
                    src.ORG_UDFNUM02, 
                    src.ORG_UDFNUM03, 
                    src.ORG_UDFNUM04, 
                    src.ORG_UDFNUM05, 
                    src.ORG_UPDATECOUNT, 
                    src.ORG_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ORGANIZATION_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ORG_ACCOUNTINGENTITY, 
            strm.ORG_BOOKID, 
            strm.ORG_CALGROUPCODE, 
            strm.ORG_CALGROUPORG, 
            strm.ORG_CODE, 
            strm.ORG_CODEREF, 
            strm.ORG_COMMON, 
            strm.ORG_CREATED, 
            strm.ORG_CURR, 
            strm.ORG_DEPMETHOD, 
            strm.ORG_DEPRMETHOD, 
            strm.ORG_DESC, 
            strm.ORG_DUNSNUM, 
            strm.ORG_EXTERNALORG, 
            strm.ORG_INVQTYTOL, 
            strm.ORG_LASTSAVED, 
            strm.ORG_LATITUDE, 
            strm.ORG_LOCALE, 
            strm.ORG_LONGITUDE, 
            strm.ORG_MATCHLTA, 
            strm.ORG_MATCHLTP, 
            strm.ORG_REQUESTRECALCDEP, 
            strm.ORG_SEGMENTVALUE, 
            strm.ORG_STREAMPLUSPROJECT, 
            strm.ORG_TIMEZONE, 
            strm.ORG_UDFCHAR01, 
            strm.ORG_UDFCHAR02, 
            strm.ORG_UDFCHAR03, 
            strm.ORG_UDFCHAR04, 
            strm.ORG_UDFCHAR05, 
            strm.ORG_UDFCHAR06, 
            strm.ORG_UDFCHAR07, 
            strm.ORG_UDFCHAR08, 
            strm.ORG_UDFCHAR09, 
            strm.ORG_UDFCHAR10, 
            strm.ORG_UDFCHKBOX01, 
            strm.ORG_UDFCHKBOX02, 
            strm.ORG_UDFCHKBOX03, 
            strm.ORG_UDFCHKBOX04, 
            strm.ORG_UDFCHKBOX05, 
            strm.ORG_UDFDATE01, 
            strm.ORG_UDFDATE02, 
            strm.ORG_UDFDATE03, 
            strm.ORG_UDFDATE04, 
            strm.ORG_UDFDATE05, 
            strm.ORG_UDFNUM01, 
            strm.ORG_UDFNUM02, 
            strm.ORG_UDFNUM03, 
            strm.ORG_UDFNUM04, 
            strm.ORG_UDFNUM05, 
            strm.ORG_UPDATECOUNT, 
            strm.ORG_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ORG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ORGANIZATION as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ORG_CODE=src.ORG_CODE) OR (target.ORG_CODE IS NULL AND src.ORG_CODE IS NULL)) 
                when matched then update set
                    target.ORG_ACCOUNTINGENTITY=src.ORG_ACCOUNTINGENTITY, 
                    target.ORG_BOOKID=src.ORG_BOOKID, 
                    target.ORG_CALGROUPCODE=src.ORG_CALGROUPCODE, 
                    target.ORG_CALGROUPORG=src.ORG_CALGROUPORG, 
                    target.ORG_CODE=src.ORG_CODE, 
                    target.ORG_CODEREF=src.ORG_CODEREF, 
                    target.ORG_COMMON=src.ORG_COMMON, 
                    target.ORG_CREATED=src.ORG_CREATED, 
                    target.ORG_CURR=src.ORG_CURR, 
                    target.ORG_DEPMETHOD=src.ORG_DEPMETHOD, 
                    target.ORG_DEPRMETHOD=src.ORG_DEPRMETHOD, 
                    target.ORG_DESC=src.ORG_DESC, 
                    target.ORG_DUNSNUM=src.ORG_DUNSNUM, 
                    target.ORG_EXTERNALORG=src.ORG_EXTERNALORG, 
                    target.ORG_INVQTYTOL=src.ORG_INVQTYTOL, 
                    target.ORG_LASTSAVED=src.ORG_LASTSAVED, 
                    target.ORG_LATITUDE=src.ORG_LATITUDE, 
                    target.ORG_LOCALE=src.ORG_LOCALE, 
                    target.ORG_LONGITUDE=src.ORG_LONGITUDE, 
                    target.ORG_MATCHLTA=src.ORG_MATCHLTA, 
                    target.ORG_MATCHLTP=src.ORG_MATCHLTP, 
                    target.ORG_REQUESTRECALCDEP=src.ORG_REQUESTRECALCDEP, 
                    target.ORG_SEGMENTVALUE=src.ORG_SEGMENTVALUE, 
                    target.ORG_STREAMPLUSPROJECT=src.ORG_STREAMPLUSPROJECT, 
                    target.ORG_TIMEZONE=src.ORG_TIMEZONE, 
                    target.ORG_UDFCHAR01=src.ORG_UDFCHAR01, 
                    target.ORG_UDFCHAR02=src.ORG_UDFCHAR02, 
                    target.ORG_UDFCHAR03=src.ORG_UDFCHAR03, 
                    target.ORG_UDFCHAR04=src.ORG_UDFCHAR04, 
                    target.ORG_UDFCHAR05=src.ORG_UDFCHAR05, 
                    target.ORG_UDFCHAR06=src.ORG_UDFCHAR06, 
                    target.ORG_UDFCHAR07=src.ORG_UDFCHAR07, 
                    target.ORG_UDFCHAR08=src.ORG_UDFCHAR08, 
                    target.ORG_UDFCHAR09=src.ORG_UDFCHAR09, 
                    target.ORG_UDFCHAR10=src.ORG_UDFCHAR10, 
                    target.ORG_UDFCHKBOX01=src.ORG_UDFCHKBOX01, 
                    target.ORG_UDFCHKBOX02=src.ORG_UDFCHKBOX02, 
                    target.ORG_UDFCHKBOX03=src.ORG_UDFCHKBOX03, 
                    target.ORG_UDFCHKBOX04=src.ORG_UDFCHKBOX04, 
                    target.ORG_UDFCHKBOX05=src.ORG_UDFCHKBOX05, 
                    target.ORG_UDFDATE01=src.ORG_UDFDATE01, 
                    target.ORG_UDFDATE02=src.ORG_UDFDATE02, 
                    target.ORG_UDFDATE03=src.ORG_UDFDATE03, 
                    target.ORG_UDFDATE04=src.ORG_UDFDATE04, 
                    target.ORG_UDFDATE05=src.ORG_UDFDATE05, 
                    target.ORG_UDFNUM01=src.ORG_UDFNUM01, 
                    target.ORG_UDFNUM02=src.ORG_UDFNUM02, 
                    target.ORG_UDFNUM03=src.ORG_UDFNUM03, 
                    target.ORG_UDFNUM04=src.ORG_UDFNUM04, 
                    target.ORG_UDFNUM05=src.ORG_UDFNUM05, 
                    target.ORG_UPDATECOUNT=src.ORG_UPDATECOUNT, 
                    target.ORG_UPDATED=src.ORG_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ORG_ACCOUNTINGENTITY, ORG_BOOKID, ORG_CALGROUPCODE, ORG_CALGROUPORG, ORG_CODE, ORG_CODEREF, ORG_COMMON, ORG_CREATED, ORG_CURR, ORG_DEPMETHOD, ORG_DEPRMETHOD, ORG_DESC, ORG_DUNSNUM, ORG_EXTERNALORG, ORG_INVQTYTOL, ORG_LASTSAVED, ORG_LATITUDE, ORG_LOCALE, ORG_LONGITUDE, ORG_MATCHLTA, ORG_MATCHLTP, ORG_REQUESTRECALCDEP, ORG_SEGMENTVALUE, ORG_STREAMPLUSPROJECT, ORG_TIMEZONE, ORG_UDFCHAR01, ORG_UDFCHAR02, ORG_UDFCHAR03, ORG_UDFCHAR04, ORG_UDFCHAR05, ORG_UDFCHAR06, ORG_UDFCHAR07, ORG_UDFCHAR08, ORG_UDFCHAR09, ORG_UDFCHAR10, ORG_UDFCHKBOX01, ORG_UDFCHKBOX02, ORG_UDFCHKBOX03, ORG_UDFCHKBOX04, ORG_UDFCHKBOX05, ORG_UDFDATE01, ORG_UDFDATE02, ORG_UDFDATE03, ORG_UDFDATE04, ORG_UDFDATE05, ORG_UDFNUM01, ORG_UDFNUM02, ORG_UDFNUM03, ORG_UDFNUM04, ORG_UDFNUM05, ORG_UPDATECOUNT, ORG_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ORG_ACCOUNTINGENTITY, 
                    src.ORG_BOOKID, 
                    src.ORG_CALGROUPCODE, 
                    src.ORG_CALGROUPORG, 
                    src.ORG_CODE, 
                    src.ORG_CODEREF, 
                    src.ORG_COMMON, 
                    src.ORG_CREATED, 
                    src.ORG_CURR, 
                    src.ORG_DEPMETHOD, 
                    src.ORG_DEPRMETHOD, 
                    src.ORG_DESC, 
                    src.ORG_DUNSNUM, 
                    src.ORG_EXTERNALORG, 
                    src.ORG_INVQTYTOL, 
                    src.ORG_LASTSAVED, 
                    src.ORG_LATITUDE, 
                    src.ORG_LOCALE, 
                    src.ORG_LONGITUDE, 
                    src.ORG_MATCHLTA, 
                    src.ORG_MATCHLTP, 
                    src.ORG_REQUESTRECALCDEP, 
                    src.ORG_SEGMENTVALUE, 
                    src.ORG_STREAMPLUSPROJECT, 
                    src.ORG_TIMEZONE, 
                    src.ORG_UDFCHAR01, 
                    src.ORG_UDFCHAR02, 
                    src.ORG_UDFCHAR03, 
                    src.ORG_UDFCHAR04, 
                    src.ORG_UDFCHAR05, 
                    src.ORG_UDFCHAR06, 
                    src.ORG_UDFCHAR07, 
                    src.ORG_UDFCHAR08, 
                    src.ORG_UDFCHAR09, 
                    src.ORG_UDFCHAR10, 
                    src.ORG_UDFCHKBOX01, 
                    src.ORG_UDFCHKBOX02, 
                    src.ORG_UDFCHKBOX03, 
                    src.ORG_UDFCHKBOX04, 
                    src.ORG_UDFCHKBOX05, 
                    src.ORG_UDFDATE01, 
                    src.ORG_UDFDATE02, 
                    src.ORG_UDFDATE03, 
                    src.ORG_UDFDATE04, 
                    src.ORG_UDFDATE05, 
                    src.ORG_UDFNUM01, 
                    src.ORG_UDFNUM02, 
                    src.ORG_UDFNUM03, 
                    src.ORG_UDFNUM04, 
                    src.ORG_UDFNUM05, 
                    src.ORG_UPDATECOUNT, 
                    src.ORG_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename,
                    case when ETL_DELETED=TRUE then TRUE else FALSE end,
                    ETL_DELETED);
    `
} );

                    
//  Get number of rows inserted
        
        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();
        var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
        var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();
                    

        
    snowflake.execute ({sqlText: "commit"});

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', 0 ," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
    snowflake.execute({sqlText: log_sql_command});

    result = "Success"; 

    }       

    catch (err)  {
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
    //  Create a log entry to say INSERT STATEMENT failed
    
    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', 0 ,0,0);";
    snowflake.execute({sqlText: log_sql_command});
        ;
        throw "Failed: " + err.message;   // Return a success/error indicator.
        }
    return result;
    $$;