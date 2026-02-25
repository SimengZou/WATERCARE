create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ADJUSTMENTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ADJUSTMENTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ADJUSTMENTS as target using (SELECT * FROM (SELECT 
            strm.ADJ_CODE, 
            strm.ADJ_DESC, 
            strm.ADJ_LASTSAVED, 
            strm.ADJ_NOTUSED, 
            strm.ADJ_ORG, 
            strm.ADJ_RATE, 
            strm.ADJ_STWO, 
            strm.ADJ_UDFCHAR01, 
            strm.ADJ_UDFCHAR02, 
            strm.ADJ_UDFCHAR03, 
            strm.ADJ_UDFCHAR04, 
            strm.ADJ_UDFCHAR05, 
            strm.ADJ_UDFCHAR06, 
            strm.ADJ_UDFCHAR07, 
            strm.ADJ_UDFCHAR08, 
            strm.ADJ_UDFCHAR09, 
            strm.ADJ_UDFCHAR10, 
            strm.ADJ_UDFCHAR11, 
            strm.ADJ_UDFCHAR12, 
            strm.ADJ_UDFCHAR13, 
            strm.ADJ_UDFCHAR14, 
            strm.ADJ_UDFCHAR15, 
            strm.ADJ_UDFCHAR16, 
            strm.ADJ_UDFCHAR17, 
            strm.ADJ_UDFCHAR18, 
            strm.ADJ_UDFCHAR19, 
            strm.ADJ_UDFCHAR20, 
            strm.ADJ_UDFCHAR21, 
            strm.ADJ_UDFCHAR22, 
            strm.ADJ_UDFCHAR23, 
            strm.ADJ_UDFCHAR24, 
            strm.ADJ_UDFCHAR25, 
            strm.ADJ_UDFCHAR26, 
            strm.ADJ_UDFCHAR27, 
            strm.ADJ_UDFCHAR28, 
            strm.ADJ_UDFCHAR29, 
            strm.ADJ_UDFCHAR30, 
            strm.ADJ_UDFCHKBOX01, 
            strm.ADJ_UDFCHKBOX02, 
            strm.ADJ_UDFCHKBOX03, 
            strm.ADJ_UDFCHKBOX04, 
            strm.ADJ_UDFCHKBOX05, 
            strm.ADJ_UDFDATE01, 
            strm.ADJ_UDFDATE02, 
            strm.ADJ_UDFDATE03, 
            strm.ADJ_UDFDATE04, 
            strm.ADJ_UDFDATE05, 
            strm.ADJ_UDFNUM01, 
            strm.ADJ_UDFNUM02, 
            strm.ADJ_UDFNUM03, 
            strm.ADJ_UDFNUM04, 
            strm.ADJ_UDFNUM05, 
            strm.ADJ_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADJ_CODE,
            strm.ADJ_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADJUSTMENTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADJ_CODE=src.ADJ_CODE) OR (target.ADJ_CODE IS NULL AND src.ADJ_CODE IS NULL)) AND
            ((target.ADJ_ORG=src.ADJ_ORG) OR (target.ADJ_ORG IS NULL AND src.ADJ_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADJ_CODE=src.ADJ_CODE, 
                    target.ADJ_DESC=src.ADJ_DESC, 
                    target.ADJ_LASTSAVED=src.ADJ_LASTSAVED, 
                    target.ADJ_NOTUSED=src.ADJ_NOTUSED, 
                    target.ADJ_ORG=src.ADJ_ORG, 
                    target.ADJ_RATE=src.ADJ_RATE, 
                    target.ADJ_STWO=src.ADJ_STWO, 
                    target.ADJ_UDFCHAR01=src.ADJ_UDFCHAR01, 
                    target.ADJ_UDFCHAR02=src.ADJ_UDFCHAR02, 
                    target.ADJ_UDFCHAR03=src.ADJ_UDFCHAR03, 
                    target.ADJ_UDFCHAR04=src.ADJ_UDFCHAR04, 
                    target.ADJ_UDFCHAR05=src.ADJ_UDFCHAR05, 
                    target.ADJ_UDFCHAR06=src.ADJ_UDFCHAR06, 
                    target.ADJ_UDFCHAR07=src.ADJ_UDFCHAR07, 
                    target.ADJ_UDFCHAR08=src.ADJ_UDFCHAR08, 
                    target.ADJ_UDFCHAR09=src.ADJ_UDFCHAR09, 
                    target.ADJ_UDFCHAR10=src.ADJ_UDFCHAR10, 
                    target.ADJ_UDFCHAR11=src.ADJ_UDFCHAR11, 
                    target.ADJ_UDFCHAR12=src.ADJ_UDFCHAR12, 
                    target.ADJ_UDFCHAR13=src.ADJ_UDFCHAR13, 
                    target.ADJ_UDFCHAR14=src.ADJ_UDFCHAR14, 
                    target.ADJ_UDFCHAR15=src.ADJ_UDFCHAR15, 
                    target.ADJ_UDFCHAR16=src.ADJ_UDFCHAR16, 
                    target.ADJ_UDFCHAR17=src.ADJ_UDFCHAR17, 
                    target.ADJ_UDFCHAR18=src.ADJ_UDFCHAR18, 
                    target.ADJ_UDFCHAR19=src.ADJ_UDFCHAR19, 
                    target.ADJ_UDFCHAR20=src.ADJ_UDFCHAR20, 
                    target.ADJ_UDFCHAR21=src.ADJ_UDFCHAR21, 
                    target.ADJ_UDFCHAR22=src.ADJ_UDFCHAR22, 
                    target.ADJ_UDFCHAR23=src.ADJ_UDFCHAR23, 
                    target.ADJ_UDFCHAR24=src.ADJ_UDFCHAR24, 
                    target.ADJ_UDFCHAR25=src.ADJ_UDFCHAR25, 
                    target.ADJ_UDFCHAR26=src.ADJ_UDFCHAR26, 
                    target.ADJ_UDFCHAR27=src.ADJ_UDFCHAR27, 
                    target.ADJ_UDFCHAR28=src.ADJ_UDFCHAR28, 
                    target.ADJ_UDFCHAR29=src.ADJ_UDFCHAR29, 
                    target.ADJ_UDFCHAR30=src.ADJ_UDFCHAR30, 
                    target.ADJ_UDFCHKBOX01=src.ADJ_UDFCHKBOX01, 
                    target.ADJ_UDFCHKBOX02=src.ADJ_UDFCHKBOX02, 
                    target.ADJ_UDFCHKBOX03=src.ADJ_UDFCHKBOX03, 
                    target.ADJ_UDFCHKBOX04=src.ADJ_UDFCHKBOX04, 
                    target.ADJ_UDFCHKBOX05=src.ADJ_UDFCHKBOX05, 
                    target.ADJ_UDFDATE01=src.ADJ_UDFDATE01, 
                    target.ADJ_UDFDATE02=src.ADJ_UDFDATE02, 
                    target.ADJ_UDFDATE03=src.ADJ_UDFDATE03, 
                    target.ADJ_UDFDATE04=src.ADJ_UDFDATE04, 
                    target.ADJ_UDFDATE05=src.ADJ_UDFDATE05, 
                    target.ADJ_UDFNUM01=src.ADJ_UDFNUM01, 
                    target.ADJ_UDFNUM02=src.ADJ_UDFNUM02, 
                    target.ADJ_UDFNUM03=src.ADJ_UDFNUM03, 
                    target.ADJ_UDFNUM04=src.ADJ_UDFNUM04, 
                    target.ADJ_UDFNUM05=src.ADJ_UDFNUM05, 
                    target.ADJ_UPDATECOUNT=src.ADJ_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADJ_CODE, 
                    ADJ_DESC, 
                    ADJ_LASTSAVED, 
                    ADJ_NOTUSED, 
                    ADJ_ORG, 
                    ADJ_RATE, 
                    ADJ_STWO, 
                    ADJ_UDFCHAR01, 
                    ADJ_UDFCHAR02, 
                    ADJ_UDFCHAR03, 
                    ADJ_UDFCHAR04, 
                    ADJ_UDFCHAR05, 
                    ADJ_UDFCHAR06, 
                    ADJ_UDFCHAR07, 
                    ADJ_UDFCHAR08, 
                    ADJ_UDFCHAR09, 
                    ADJ_UDFCHAR10, 
                    ADJ_UDFCHAR11, 
                    ADJ_UDFCHAR12, 
                    ADJ_UDFCHAR13, 
                    ADJ_UDFCHAR14, 
                    ADJ_UDFCHAR15, 
                    ADJ_UDFCHAR16, 
                    ADJ_UDFCHAR17, 
                    ADJ_UDFCHAR18, 
                    ADJ_UDFCHAR19, 
                    ADJ_UDFCHAR20, 
                    ADJ_UDFCHAR21, 
                    ADJ_UDFCHAR22, 
                    ADJ_UDFCHAR23, 
                    ADJ_UDFCHAR24, 
                    ADJ_UDFCHAR25, 
                    ADJ_UDFCHAR26, 
                    ADJ_UDFCHAR27, 
                    ADJ_UDFCHAR28, 
                    ADJ_UDFCHAR29, 
                    ADJ_UDFCHAR30, 
                    ADJ_UDFCHKBOX01, 
                    ADJ_UDFCHKBOX02, 
                    ADJ_UDFCHKBOX03, 
                    ADJ_UDFCHKBOX04, 
                    ADJ_UDFCHKBOX05, 
                    ADJ_UDFDATE01, 
                    ADJ_UDFDATE02, 
                    ADJ_UDFDATE03, 
                    ADJ_UDFDATE04, 
                    ADJ_UDFDATE05, 
                    ADJ_UDFNUM01, 
                    ADJ_UDFNUM02, 
                    ADJ_UDFNUM03, 
                    ADJ_UDFNUM04, 
                    ADJ_UDFNUM05, 
                    ADJ_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADJ_CODE, 
                    src.ADJ_DESC, 
                    src.ADJ_LASTSAVED, 
                    src.ADJ_NOTUSED, 
                    src.ADJ_ORG, 
                    src.ADJ_RATE, 
                    src.ADJ_STWO, 
                    src.ADJ_UDFCHAR01, 
                    src.ADJ_UDFCHAR02, 
                    src.ADJ_UDFCHAR03, 
                    src.ADJ_UDFCHAR04, 
                    src.ADJ_UDFCHAR05, 
                    src.ADJ_UDFCHAR06, 
                    src.ADJ_UDFCHAR07, 
                    src.ADJ_UDFCHAR08, 
                    src.ADJ_UDFCHAR09, 
                    src.ADJ_UDFCHAR10, 
                    src.ADJ_UDFCHAR11, 
                    src.ADJ_UDFCHAR12, 
                    src.ADJ_UDFCHAR13, 
                    src.ADJ_UDFCHAR14, 
                    src.ADJ_UDFCHAR15, 
                    src.ADJ_UDFCHAR16, 
                    src.ADJ_UDFCHAR17, 
                    src.ADJ_UDFCHAR18, 
                    src.ADJ_UDFCHAR19, 
                    src.ADJ_UDFCHAR20, 
                    src.ADJ_UDFCHAR21, 
                    src.ADJ_UDFCHAR22, 
                    src.ADJ_UDFCHAR23, 
                    src.ADJ_UDFCHAR24, 
                    src.ADJ_UDFCHAR25, 
                    src.ADJ_UDFCHAR26, 
                    src.ADJ_UDFCHAR27, 
                    src.ADJ_UDFCHAR28, 
                    src.ADJ_UDFCHAR29, 
                    src.ADJ_UDFCHAR30, 
                    src.ADJ_UDFCHKBOX01, 
                    src.ADJ_UDFCHKBOX02, 
                    src.ADJ_UDFCHKBOX03, 
                    src.ADJ_UDFCHKBOX04, 
                    src.ADJ_UDFCHKBOX05, 
                    src.ADJ_UDFDATE01, 
                    src.ADJ_UDFDATE02, 
                    src.ADJ_UDFDATE03, 
                    src.ADJ_UDFDATE04, 
                    src.ADJ_UDFDATE05, 
                    src.ADJ_UDFNUM01, 
                    src.ADJ_UDFNUM02, 
                    src.ADJ_UDFNUM03, 
                    src.ADJ_UDFNUM04, 
                    src.ADJ_UDFNUM05, 
                    src.ADJ_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ADJUSTMENTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ADJ_CODE, 
            strm.ADJ_DESC, 
            strm.ADJ_LASTSAVED, 
            strm.ADJ_NOTUSED, 
            strm.ADJ_ORG, 
            strm.ADJ_RATE, 
            strm.ADJ_STWO, 
            strm.ADJ_UDFCHAR01, 
            strm.ADJ_UDFCHAR02, 
            strm.ADJ_UDFCHAR03, 
            strm.ADJ_UDFCHAR04, 
            strm.ADJ_UDFCHAR05, 
            strm.ADJ_UDFCHAR06, 
            strm.ADJ_UDFCHAR07, 
            strm.ADJ_UDFCHAR08, 
            strm.ADJ_UDFCHAR09, 
            strm.ADJ_UDFCHAR10, 
            strm.ADJ_UDFCHAR11, 
            strm.ADJ_UDFCHAR12, 
            strm.ADJ_UDFCHAR13, 
            strm.ADJ_UDFCHAR14, 
            strm.ADJ_UDFCHAR15, 
            strm.ADJ_UDFCHAR16, 
            strm.ADJ_UDFCHAR17, 
            strm.ADJ_UDFCHAR18, 
            strm.ADJ_UDFCHAR19, 
            strm.ADJ_UDFCHAR20, 
            strm.ADJ_UDFCHAR21, 
            strm.ADJ_UDFCHAR22, 
            strm.ADJ_UDFCHAR23, 
            strm.ADJ_UDFCHAR24, 
            strm.ADJ_UDFCHAR25, 
            strm.ADJ_UDFCHAR26, 
            strm.ADJ_UDFCHAR27, 
            strm.ADJ_UDFCHAR28, 
            strm.ADJ_UDFCHAR29, 
            strm.ADJ_UDFCHAR30, 
            strm.ADJ_UDFCHKBOX01, 
            strm.ADJ_UDFCHKBOX02, 
            strm.ADJ_UDFCHKBOX03, 
            strm.ADJ_UDFCHKBOX04, 
            strm.ADJ_UDFCHKBOX05, 
            strm.ADJ_UDFDATE01, 
            strm.ADJ_UDFDATE02, 
            strm.ADJ_UDFDATE03, 
            strm.ADJ_UDFDATE04, 
            strm.ADJ_UDFDATE05, 
            strm.ADJ_UDFNUM01, 
            strm.ADJ_UDFNUM02, 
            strm.ADJ_UDFNUM03, 
            strm.ADJ_UDFNUM04, 
            strm.ADJ_UDFNUM05, 
            strm.ADJ_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADJ_CODE,
            strm.ADJ_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADJUSTMENTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADJ_CODE=src.ADJ_CODE) OR (target.ADJ_CODE IS NULL AND src.ADJ_CODE IS NULL)) AND
            ((target.ADJ_ORG=src.ADJ_ORG) OR (target.ADJ_ORG IS NULL AND src.ADJ_ORG IS NULL)) 
                when matched then update set
                    target.ADJ_CODE=src.ADJ_CODE, 
                    target.ADJ_DESC=src.ADJ_DESC, 
                    target.ADJ_LASTSAVED=src.ADJ_LASTSAVED, 
                    target.ADJ_NOTUSED=src.ADJ_NOTUSED, 
                    target.ADJ_ORG=src.ADJ_ORG, 
                    target.ADJ_RATE=src.ADJ_RATE, 
                    target.ADJ_STWO=src.ADJ_STWO, 
                    target.ADJ_UDFCHAR01=src.ADJ_UDFCHAR01, 
                    target.ADJ_UDFCHAR02=src.ADJ_UDFCHAR02, 
                    target.ADJ_UDFCHAR03=src.ADJ_UDFCHAR03, 
                    target.ADJ_UDFCHAR04=src.ADJ_UDFCHAR04, 
                    target.ADJ_UDFCHAR05=src.ADJ_UDFCHAR05, 
                    target.ADJ_UDFCHAR06=src.ADJ_UDFCHAR06, 
                    target.ADJ_UDFCHAR07=src.ADJ_UDFCHAR07, 
                    target.ADJ_UDFCHAR08=src.ADJ_UDFCHAR08, 
                    target.ADJ_UDFCHAR09=src.ADJ_UDFCHAR09, 
                    target.ADJ_UDFCHAR10=src.ADJ_UDFCHAR10, 
                    target.ADJ_UDFCHAR11=src.ADJ_UDFCHAR11, 
                    target.ADJ_UDFCHAR12=src.ADJ_UDFCHAR12, 
                    target.ADJ_UDFCHAR13=src.ADJ_UDFCHAR13, 
                    target.ADJ_UDFCHAR14=src.ADJ_UDFCHAR14, 
                    target.ADJ_UDFCHAR15=src.ADJ_UDFCHAR15, 
                    target.ADJ_UDFCHAR16=src.ADJ_UDFCHAR16, 
                    target.ADJ_UDFCHAR17=src.ADJ_UDFCHAR17, 
                    target.ADJ_UDFCHAR18=src.ADJ_UDFCHAR18, 
                    target.ADJ_UDFCHAR19=src.ADJ_UDFCHAR19, 
                    target.ADJ_UDFCHAR20=src.ADJ_UDFCHAR20, 
                    target.ADJ_UDFCHAR21=src.ADJ_UDFCHAR21, 
                    target.ADJ_UDFCHAR22=src.ADJ_UDFCHAR22, 
                    target.ADJ_UDFCHAR23=src.ADJ_UDFCHAR23, 
                    target.ADJ_UDFCHAR24=src.ADJ_UDFCHAR24, 
                    target.ADJ_UDFCHAR25=src.ADJ_UDFCHAR25, 
                    target.ADJ_UDFCHAR26=src.ADJ_UDFCHAR26, 
                    target.ADJ_UDFCHAR27=src.ADJ_UDFCHAR27, 
                    target.ADJ_UDFCHAR28=src.ADJ_UDFCHAR28, 
                    target.ADJ_UDFCHAR29=src.ADJ_UDFCHAR29, 
                    target.ADJ_UDFCHAR30=src.ADJ_UDFCHAR30, 
                    target.ADJ_UDFCHKBOX01=src.ADJ_UDFCHKBOX01, 
                    target.ADJ_UDFCHKBOX02=src.ADJ_UDFCHKBOX02, 
                    target.ADJ_UDFCHKBOX03=src.ADJ_UDFCHKBOX03, 
                    target.ADJ_UDFCHKBOX04=src.ADJ_UDFCHKBOX04, 
                    target.ADJ_UDFCHKBOX05=src.ADJ_UDFCHKBOX05, 
                    target.ADJ_UDFDATE01=src.ADJ_UDFDATE01, 
                    target.ADJ_UDFDATE02=src.ADJ_UDFDATE02, 
                    target.ADJ_UDFDATE03=src.ADJ_UDFDATE03, 
                    target.ADJ_UDFDATE04=src.ADJ_UDFDATE04, 
                    target.ADJ_UDFDATE05=src.ADJ_UDFDATE05, 
                    target.ADJ_UDFNUM01=src.ADJ_UDFNUM01, 
                    target.ADJ_UDFNUM02=src.ADJ_UDFNUM02, 
                    target.ADJ_UDFNUM03=src.ADJ_UDFNUM03, 
                    target.ADJ_UDFNUM04=src.ADJ_UDFNUM04, 
                    target.ADJ_UDFNUM05=src.ADJ_UDFNUM05, 
                    target.ADJ_UPDATECOUNT=src.ADJ_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADJ_CODE, ADJ_DESC, ADJ_LASTSAVED, ADJ_NOTUSED, ADJ_ORG, ADJ_RATE, ADJ_STWO, ADJ_UDFCHAR01, ADJ_UDFCHAR02, ADJ_UDFCHAR03, ADJ_UDFCHAR04, ADJ_UDFCHAR05, ADJ_UDFCHAR06, ADJ_UDFCHAR07, ADJ_UDFCHAR08, ADJ_UDFCHAR09, ADJ_UDFCHAR10, ADJ_UDFCHAR11, ADJ_UDFCHAR12, ADJ_UDFCHAR13, ADJ_UDFCHAR14, ADJ_UDFCHAR15, ADJ_UDFCHAR16, ADJ_UDFCHAR17, ADJ_UDFCHAR18, ADJ_UDFCHAR19, ADJ_UDFCHAR20, ADJ_UDFCHAR21, ADJ_UDFCHAR22, ADJ_UDFCHAR23, ADJ_UDFCHAR24, ADJ_UDFCHAR25, ADJ_UDFCHAR26, ADJ_UDFCHAR27, ADJ_UDFCHAR28, ADJ_UDFCHAR29, ADJ_UDFCHAR30, ADJ_UDFCHKBOX01, ADJ_UDFCHKBOX02, ADJ_UDFCHKBOX03, ADJ_UDFCHKBOX04, ADJ_UDFCHKBOX05, ADJ_UDFDATE01, ADJ_UDFDATE02, ADJ_UDFDATE03, ADJ_UDFDATE04, ADJ_UDFDATE05, ADJ_UDFNUM01, ADJ_UDFNUM02, ADJ_UDFNUM03, ADJ_UDFNUM04, ADJ_UDFNUM05, ADJ_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADJ_CODE, 
                    src.ADJ_DESC, 
                    src.ADJ_LASTSAVED, 
                    src.ADJ_NOTUSED, 
                    src.ADJ_ORG, 
                    src.ADJ_RATE, 
                    src.ADJ_STWO, 
                    src.ADJ_UDFCHAR01, 
                    src.ADJ_UDFCHAR02, 
                    src.ADJ_UDFCHAR03, 
                    src.ADJ_UDFCHAR04, 
                    src.ADJ_UDFCHAR05, 
                    src.ADJ_UDFCHAR06, 
                    src.ADJ_UDFCHAR07, 
                    src.ADJ_UDFCHAR08, 
                    src.ADJ_UDFCHAR09, 
                    src.ADJ_UDFCHAR10, 
                    src.ADJ_UDFCHAR11, 
                    src.ADJ_UDFCHAR12, 
                    src.ADJ_UDFCHAR13, 
                    src.ADJ_UDFCHAR14, 
                    src.ADJ_UDFCHAR15, 
                    src.ADJ_UDFCHAR16, 
                    src.ADJ_UDFCHAR17, 
                    src.ADJ_UDFCHAR18, 
                    src.ADJ_UDFCHAR19, 
                    src.ADJ_UDFCHAR20, 
                    src.ADJ_UDFCHAR21, 
                    src.ADJ_UDFCHAR22, 
                    src.ADJ_UDFCHAR23, 
                    src.ADJ_UDFCHAR24, 
                    src.ADJ_UDFCHAR25, 
                    src.ADJ_UDFCHAR26, 
                    src.ADJ_UDFCHAR27, 
                    src.ADJ_UDFCHAR28, 
                    src.ADJ_UDFCHAR29, 
                    src.ADJ_UDFCHAR30, 
                    src.ADJ_UDFCHKBOX01, 
                    src.ADJ_UDFCHKBOX02, 
                    src.ADJ_UDFCHKBOX03, 
                    src.ADJ_UDFCHKBOX04, 
                    src.ADJ_UDFCHKBOX05, 
                    src.ADJ_UDFDATE01, 
                    src.ADJ_UDFDATE02, 
                    src.ADJ_UDFDATE03, 
                    src.ADJ_UDFDATE04, 
                    src.ADJ_UDFDATE05, 
                    src.ADJ_UDFNUM01, 
                    src.ADJ_UDFNUM02, 
                    src.ADJ_UDFNUM03, 
                    src.ADJ_UDFNUM04, 
                    src.ADJ_UDFNUM05, 
                    src.ADJ_UPDATECOUNT, 
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