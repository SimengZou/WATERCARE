create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5TRACKINGPROMPTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5TRACKINGPROMPTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5TRACKINGPROMPTS as target using (SELECT * FROM (SELECT 
            strm.TKP_ARCCOLUMN, 
            strm.TKP_ARCHIVECOLUMN, 
            strm.TKP_ARCRCOLUMN, 
            strm.TKP_BRANCHCONDITION, 
            strm.TKP_BRANCHGOTO, 
            strm.TKP_BRANCHPATTERN, 
            strm.TKP_COLUMN, 
            strm.TKP_COMPUTEDATA, 
            strm.TKP_DATARTYPE, 
            strm.TKP_DATATYPE, 
            strm.TKP_DATEFORMAT, 
            strm.TKP_DEFAULTPREVDATA, 
            strm.TKP_EWSQUERYCODE, 
            strm.TKP_FIXEDDATA, 
            strm.TKP_INTERFACERTYPE, 
            strm.TKP_INTERFACETYPE, 
            strm.TKP_LASTSAVED, 
            strm.TKP_LOVID, 
            strm.TKP_LOVOVERRIDEFLAG, 
            strm.TKP_MATCHPATTERN, 
            strm.TKP_MAXSIZE, 
            strm.TKP_MINSIZE, 
            strm.TKP_NOTONFILEFLAG, 
            strm.TKP_OBTRANSRTYPE, 
            strm.TKP_OBTRANSTYPE, 
            strm.TKP_PROMPT, 
            strm.TKP_PROMPTSEQ, 
            strm.TKP_RCOLUMN, 
            strm.TKP_RETURNTOPROMPT, 
            strm.TKP_SQLCODE, 
            strm.TKP_TRANS, 
            strm.TKP_TRANSGROUP, 
            strm.TKP_TRANSSEQ, 
            strm.TKP_UPDATECOUNT, 
            strm.TKP_UPDATED, 
            strm.TKP_UPLOADCOLUMN, 
            strm.TKP_VALIDATEFILE, 
            strm.TKP_VALIDATELOV, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TKP_TRANS,
            strm.TKP_TRANSSEQ ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRACKINGPROMPTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TKP_TRANS=src.TKP_TRANS) OR (target.TKP_TRANS IS NULL AND src.TKP_TRANS IS NULL)) AND
            ((target.TKP_TRANSSEQ=src.TKP_TRANSSEQ) OR (target.TKP_TRANSSEQ IS NULL AND src.TKP_TRANSSEQ IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.TKP_ARCCOLUMN=src.TKP_ARCCOLUMN, 
                    target.TKP_ARCHIVECOLUMN=src.TKP_ARCHIVECOLUMN, 
                    target.TKP_ARCRCOLUMN=src.TKP_ARCRCOLUMN, 
                    target.TKP_BRANCHCONDITION=src.TKP_BRANCHCONDITION, 
                    target.TKP_BRANCHGOTO=src.TKP_BRANCHGOTO, 
                    target.TKP_BRANCHPATTERN=src.TKP_BRANCHPATTERN, 
                    target.TKP_COLUMN=src.TKP_COLUMN, 
                    target.TKP_COMPUTEDATA=src.TKP_COMPUTEDATA, 
                    target.TKP_DATARTYPE=src.TKP_DATARTYPE, 
                    target.TKP_DATATYPE=src.TKP_DATATYPE, 
                    target.TKP_DATEFORMAT=src.TKP_DATEFORMAT, 
                    target.TKP_DEFAULTPREVDATA=src.TKP_DEFAULTPREVDATA, 
                    target.TKP_EWSQUERYCODE=src.TKP_EWSQUERYCODE, 
                    target.TKP_FIXEDDATA=src.TKP_FIXEDDATA, 
                    target.TKP_INTERFACERTYPE=src.TKP_INTERFACERTYPE, 
                    target.TKP_INTERFACETYPE=src.TKP_INTERFACETYPE, 
                    target.TKP_LASTSAVED=src.TKP_LASTSAVED, 
                    target.TKP_LOVID=src.TKP_LOVID, 
                    target.TKP_LOVOVERRIDEFLAG=src.TKP_LOVOVERRIDEFLAG, 
                    target.TKP_MATCHPATTERN=src.TKP_MATCHPATTERN, 
                    target.TKP_MAXSIZE=src.TKP_MAXSIZE, 
                    target.TKP_MINSIZE=src.TKP_MINSIZE, 
                    target.TKP_NOTONFILEFLAG=src.TKP_NOTONFILEFLAG, 
                    target.TKP_OBTRANSRTYPE=src.TKP_OBTRANSRTYPE, 
                    target.TKP_OBTRANSTYPE=src.TKP_OBTRANSTYPE, 
                    target.TKP_PROMPT=src.TKP_PROMPT, 
                    target.TKP_PROMPTSEQ=src.TKP_PROMPTSEQ, 
                    target.TKP_RCOLUMN=src.TKP_RCOLUMN, 
                    target.TKP_RETURNTOPROMPT=src.TKP_RETURNTOPROMPT, 
                    target.TKP_SQLCODE=src.TKP_SQLCODE, 
                    target.TKP_TRANS=src.TKP_TRANS, 
                    target.TKP_TRANSGROUP=src.TKP_TRANSGROUP, 
                    target.TKP_TRANSSEQ=src.TKP_TRANSSEQ, 
                    target.TKP_UPDATECOUNT=src.TKP_UPDATECOUNT, 
                    target.TKP_UPDATED=src.TKP_UPDATED, 
                    target.TKP_UPLOADCOLUMN=src.TKP_UPLOADCOLUMN, 
                    target.TKP_VALIDATEFILE=src.TKP_VALIDATEFILE, 
                    target.TKP_VALIDATELOV=src.TKP_VALIDATELOV, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    TKP_ARCCOLUMN, 
                    TKP_ARCHIVECOLUMN, 
                    TKP_ARCRCOLUMN, 
                    TKP_BRANCHCONDITION, 
                    TKP_BRANCHGOTO, 
                    TKP_BRANCHPATTERN, 
                    TKP_COLUMN, 
                    TKP_COMPUTEDATA, 
                    TKP_DATARTYPE, 
                    TKP_DATATYPE, 
                    TKP_DATEFORMAT, 
                    TKP_DEFAULTPREVDATA, 
                    TKP_EWSQUERYCODE, 
                    TKP_FIXEDDATA, 
                    TKP_INTERFACERTYPE, 
                    TKP_INTERFACETYPE, 
                    TKP_LASTSAVED, 
                    TKP_LOVID, 
                    TKP_LOVOVERRIDEFLAG, 
                    TKP_MATCHPATTERN, 
                    TKP_MAXSIZE, 
                    TKP_MINSIZE, 
                    TKP_NOTONFILEFLAG, 
                    TKP_OBTRANSRTYPE, 
                    TKP_OBTRANSTYPE, 
                    TKP_PROMPT, 
                    TKP_PROMPTSEQ, 
                    TKP_RCOLUMN, 
                    TKP_RETURNTOPROMPT, 
                    TKP_SQLCODE, 
                    TKP_TRANS, 
                    TKP_TRANSGROUP, 
                    TKP_TRANSSEQ, 
                    TKP_UPDATECOUNT, 
                    TKP_UPDATED, 
                    TKP_UPLOADCOLUMN, 
                    TKP_VALIDATEFILE, 
                    TKP_VALIDATELOV, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.TKP_ARCCOLUMN, 
                    src.TKP_ARCHIVECOLUMN, 
                    src.TKP_ARCRCOLUMN, 
                    src.TKP_BRANCHCONDITION, 
                    src.TKP_BRANCHGOTO, 
                    src.TKP_BRANCHPATTERN, 
                    src.TKP_COLUMN, 
                    src.TKP_COMPUTEDATA, 
                    src.TKP_DATARTYPE, 
                    src.TKP_DATATYPE, 
                    src.TKP_DATEFORMAT, 
                    src.TKP_DEFAULTPREVDATA, 
                    src.TKP_EWSQUERYCODE, 
                    src.TKP_FIXEDDATA, 
                    src.TKP_INTERFACERTYPE, 
                    src.TKP_INTERFACETYPE, 
                    src.TKP_LASTSAVED, 
                    src.TKP_LOVID, 
                    src.TKP_LOVOVERRIDEFLAG, 
                    src.TKP_MATCHPATTERN, 
                    src.TKP_MAXSIZE, 
                    src.TKP_MINSIZE, 
                    src.TKP_NOTONFILEFLAG, 
                    src.TKP_OBTRANSRTYPE, 
                    src.TKP_OBTRANSTYPE, 
                    src.TKP_PROMPT, 
                    src.TKP_PROMPTSEQ, 
                    src.TKP_RCOLUMN, 
                    src.TKP_RETURNTOPROMPT, 
                    src.TKP_SQLCODE, 
                    src.TKP_TRANS, 
                    src.TKP_TRANSGROUP, 
                    src.TKP_TRANSSEQ, 
                    src.TKP_UPDATECOUNT, 
                    src.TKP_UPDATED, 
                    src.TKP_UPLOADCOLUMN, 
                    src.TKP_VALIDATEFILE, 
                    src.TKP_VALIDATELOV, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5TRACKINGPROMPTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.TKP_ARCCOLUMN, 
            strm.TKP_ARCHIVECOLUMN, 
            strm.TKP_ARCRCOLUMN, 
            strm.TKP_BRANCHCONDITION, 
            strm.TKP_BRANCHGOTO, 
            strm.TKP_BRANCHPATTERN, 
            strm.TKP_COLUMN, 
            strm.TKP_COMPUTEDATA, 
            strm.TKP_DATARTYPE, 
            strm.TKP_DATATYPE, 
            strm.TKP_DATEFORMAT, 
            strm.TKP_DEFAULTPREVDATA, 
            strm.TKP_EWSQUERYCODE, 
            strm.TKP_FIXEDDATA, 
            strm.TKP_INTERFACERTYPE, 
            strm.TKP_INTERFACETYPE, 
            strm.TKP_LASTSAVED, 
            strm.TKP_LOVID, 
            strm.TKP_LOVOVERRIDEFLAG, 
            strm.TKP_MATCHPATTERN, 
            strm.TKP_MAXSIZE, 
            strm.TKP_MINSIZE, 
            strm.TKP_NOTONFILEFLAG, 
            strm.TKP_OBTRANSRTYPE, 
            strm.TKP_OBTRANSTYPE, 
            strm.TKP_PROMPT, 
            strm.TKP_PROMPTSEQ, 
            strm.TKP_RCOLUMN, 
            strm.TKP_RETURNTOPROMPT, 
            strm.TKP_SQLCODE, 
            strm.TKP_TRANS, 
            strm.TKP_TRANSGROUP, 
            strm.TKP_TRANSSEQ, 
            strm.TKP_UPDATECOUNT, 
            strm.TKP_UPDATED, 
            strm.TKP_UPLOADCOLUMN, 
            strm.TKP_VALIDATEFILE, 
            strm.TKP_VALIDATELOV, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TKP_TRANS,
            strm.TKP_TRANSSEQ ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRACKINGPROMPTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TKP_TRANS=src.TKP_TRANS) OR (target.TKP_TRANS IS NULL AND src.TKP_TRANS IS NULL)) AND
            ((target.TKP_TRANSSEQ=src.TKP_TRANSSEQ) OR (target.TKP_TRANSSEQ IS NULL AND src.TKP_TRANSSEQ IS NULL)) 
                when matched then update set
                    target.TKP_ARCCOLUMN=src.TKP_ARCCOLUMN, 
                    target.TKP_ARCHIVECOLUMN=src.TKP_ARCHIVECOLUMN, 
                    target.TKP_ARCRCOLUMN=src.TKP_ARCRCOLUMN, 
                    target.TKP_BRANCHCONDITION=src.TKP_BRANCHCONDITION, 
                    target.TKP_BRANCHGOTO=src.TKP_BRANCHGOTO, 
                    target.TKP_BRANCHPATTERN=src.TKP_BRANCHPATTERN, 
                    target.TKP_COLUMN=src.TKP_COLUMN, 
                    target.TKP_COMPUTEDATA=src.TKP_COMPUTEDATA, 
                    target.TKP_DATARTYPE=src.TKP_DATARTYPE, 
                    target.TKP_DATATYPE=src.TKP_DATATYPE, 
                    target.TKP_DATEFORMAT=src.TKP_DATEFORMAT, 
                    target.TKP_DEFAULTPREVDATA=src.TKP_DEFAULTPREVDATA, 
                    target.TKP_EWSQUERYCODE=src.TKP_EWSQUERYCODE, 
                    target.TKP_FIXEDDATA=src.TKP_FIXEDDATA, 
                    target.TKP_INTERFACERTYPE=src.TKP_INTERFACERTYPE, 
                    target.TKP_INTERFACETYPE=src.TKP_INTERFACETYPE, 
                    target.TKP_LASTSAVED=src.TKP_LASTSAVED, 
                    target.TKP_LOVID=src.TKP_LOVID, 
                    target.TKP_LOVOVERRIDEFLAG=src.TKP_LOVOVERRIDEFLAG, 
                    target.TKP_MATCHPATTERN=src.TKP_MATCHPATTERN, 
                    target.TKP_MAXSIZE=src.TKP_MAXSIZE, 
                    target.TKP_MINSIZE=src.TKP_MINSIZE, 
                    target.TKP_NOTONFILEFLAG=src.TKP_NOTONFILEFLAG, 
                    target.TKP_OBTRANSRTYPE=src.TKP_OBTRANSRTYPE, 
                    target.TKP_OBTRANSTYPE=src.TKP_OBTRANSTYPE, 
                    target.TKP_PROMPT=src.TKP_PROMPT, 
                    target.TKP_PROMPTSEQ=src.TKP_PROMPTSEQ, 
                    target.TKP_RCOLUMN=src.TKP_RCOLUMN, 
                    target.TKP_RETURNTOPROMPT=src.TKP_RETURNTOPROMPT, 
                    target.TKP_SQLCODE=src.TKP_SQLCODE, 
                    target.TKP_TRANS=src.TKP_TRANS, 
                    target.TKP_TRANSGROUP=src.TKP_TRANSGROUP, 
                    target.TKP_TRANSSEQ=src.TKP_TRANSSEQ, 
                    target.TKP_UPDATECOUNT=src.TKP_UPDATECOUNT, 
                    target.TKP_UPDATED=src.TKP_UPDATED, 
                    target.TKP_UPLOADCOLUMN=src.TKP_UPLOADCOLUMN, 
                    target.TKP_VALIDATEFILE=src.TKP_VALIDATEFILE, 
                    target.TKP_VALIDATELOV=src.TKP_VALIDATELOV, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( TKP_ARCCOLUMN, TKP_ARCHIVECOLUMN, TKP_ARCRCOLUMN, TKP_BRANCHCONDITION, TKP_BRANCHGOTO, TKP_BRANCHPATTERN, TKP_COLUMN, TKP_COMPUTEDATA, TKP_DATARTYPE, TKP_DATATYPE, TKP_DATEFORMAT, TKP_DEFAULTPREVDATA, TKP_EWSQUERYCODE, TKP_FIXEDDATA, TKP_INTERFACERTYPE, TKP_INTERFACETYPE, TKP_LASTSAVED, TKP_LOVID, TKP_LOVOVERRIDEFLAG, TKP_MATCHPATTERN, TKP_MAXSIZE, TKP_MINSIZE, TKP_NOTONFILEFLAG, TKP_OBTRANSRTYPE, TKP_OBTRANSTYPE, TKP_PROMPT, TKP_PROMPTSEQ, TKP_RCOLUMN, TKP_RETURNTOPROMPT, TKP_SQLCODE, TKP_TRANS, TKP_TRANSGROUP, TKP_TRANSSEQ, TKP_UPDATECOUNT, TKP_UPDATED, TKP_UPLOADCOLUMN, TKP_VALIDATEFILE, TKP_VALIDATELOV, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.TKP_ARCCOLUMN, 
                    src.TKP_ARCHIVECOLUMN, 
                    src.TKP_ARCRCOLUMN, 
                    src.TKP_BRANCHCONDITION, 
                    src.TKP_BRANCHGOTO, 
                    src.TKP_BRANCHPATTERN, 
                    src.TKP_COLUMN, 
                    src.TKP_COMPUTEDATA, 
                    src.TKP_DATARTYPE, 
                    src.TKP_DATATYPE, 
                    src.TKP_DATEFORMAT, 
                    src.TKP_DEFAULTPREVDATA, 
                    src.TKP_EWSQUERYCODE, 
                    src.TKP_FIXEDDATA, 
                    src.TKP_INTERFACERTYPE, 
                    src.TKP_INTERFACETYPE, 
                    src.TKP_LASTSAVED, 
                    src.TKP_LOVID, 
                    src.TKP_LOVOVERRIDEFLAG, 
                    src.TKP_MATCHPATTERN, 
                    src.TKP_MAXSIZE, 
                    src.TKP_MINSIZE, 
                    src.TKP_NOTONFILEFLAG, 
                    src.TKP_OBTRANSRTYPE, 
                    src.TKP_OBTRANSTYPE, 
                    src.TKP_PROMPT, 
                    src.TKP_PROMPTSEQ, 
                    src.TKP_RCOLUMN, 
                    src.TKP_RETURNTOPROMPT, 
                    src.TKP_SQLCODE, 
                    src.TKP_TRANS, 
                    src.TKP_TRANSGROUP, 
                    src.TKP_TRANSSEQ, 
                    src.TKP_UPDATECOUNT, 
                    src.TKP_UPDATED, 
                    src.TKP_UPLOADCOLUMN, 
                    src.TKP_VALIDATEFILE, 
                    src.TKP_VALIDATELOV, 
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