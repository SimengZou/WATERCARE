create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PROMPTWEBSERVICES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PROMPTWEBSERVICES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PROMPTWEBSERVICES as target using (SELECT * FROM (SELECT 
            strm.PWS_ACTIONCODE, 
            strm.PWS_BOTTOMBLOCKTITLE, 
            strm.PWS_CFBLOCKTITLE, 
            strm.PWS_CFKEYCODE, 
            strm.PWS_FUNCTION, 
            strm.PWS_LASTSAVED, 
            strm.PWS_ORGXPATH, 
            strm.PWS_PROCESSGROUP, 
            strm.PWS_TAB, 
            strm.PWS_TOPBLOCKTITLE, 
            strm.PWS_UPDATECOUNT, 
            strm.PWS_UPDATED, 
            strm.PWS_WEBSERVICE, 
            strm.PWS_WSPRMCODE, 
            strm.PWS_WSTITLE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PWS_PROCESSGROUP,
            strm.PWS_WSPRMCODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROMPTWEBSERVICES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PWS_PROCESSGROUP=src.PWS_PROCESSGROUP) OR (target.PWS_PROCESSGROUP IS NULL AND src.PWS_PROCESSGROUP IS NULL)) AND
            ((target.PWS_WSPRMCODE=src.PWS_WSPRMCODE) OR (target.PWS_WSPRMCODE IS NULL AND src.PWS_WSPRMCODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PWS_ACTIONCODE=src.PWS_ACTIONCODE, 
                    target.PWS_BOTTOMBLOCKTITLE=src.PWS_BOTTOMBLOCKTITLE, 
                    target.PWS_CFBLOCKTITLE=src.PWS_CFBLOCKTITLE, 
                    target.PWS_CFKEYCODE=src.PWS_CFKEYCODE, 
                    target.PWS_FUNCTION=src.PWS_FUNCTION, 
                    target.PWS_LASTSAVED=src.PWS_LASTSAVED, 
                    target.PWS_ORGXPATH=src.PWS_ORGXPATH, 
                    target.PWS_PROCESSGROUP=src.PWS_PROCESSGROUP, 
                    target.PWS_TAB=src.PWS_TAB, 
                    target.PWS_TOPBLOCKTITLE=src.PWS_TOPBLOCKTITLE, 
                    target.PWS_UPDATECOUNT=src.PWS_UPDATECOUNT, 
                    target.PWS_UPDATED=src.PWS_UPDATED, 
                    target.PWS_WEBSERVICE=src.PWS_WEBSERVICE, 
                    target.PWS_WSPRMCODE=src.PWS_WSPRMCODE, 
                    target.PWS_WSTITLE=src.PWS_WSTITLE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PWS_ACTIONCODE, 
                    PWS_BOTTOMBLOCKTITLE, 
                    PWS_CFBLOCKTITLE, 
                    PWS_CFKEYCODE, 
                    PWS_FUNCTION, 
                    PWS_LASTSAVED, 
                    PWS_ORGXPATH, 
                    PWS_PROCESSGROUP, 
                    PWS_TAB, 
                    PWS_TOPBLOCKTITLE, 
                    PWS_UPDATECOUNT, 
                    PWS_UPDATED, 
                    PWS_WEBSERVICE, 
                    PWS_WSPRMCODE, 
                    PWS_WSTITLE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PWS_ACTIONCODE, 
                    src.PWS_BOTTOMBLOCKTITLE, 
                    src.PWS_CFBLOCKTITLE, 
                    src.PWS_CFKEYCODE, 
                    src.PWS_FUNCTION, 
                    src.PWS_LASTSAVED, 
                    src.PWS_ORGXPATH, 
                    src.PWS_PROCESSGROUP, 
                    src.PWS_TAB, 
                    src.PWS_TOPBLOCKTITLE, 
                    src.PWS_UPDATECOUNT, 
                    src.PWS_UPDATED, 
                    src.PWS_WEBSERVICE, 
                    src.PWS_WSPRMCODE, 
                    src.PWS_WSTITLE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PROMPTWEBSERVICES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PWS_ACTIONCODE, 
            strm.PWS_BOTTOMBLOCKTITLE, 
            strm.PWS_CFBLOCKTITLE, 
            strm.PWS_CFKEYCODE, 
            strm.PWS_FUNCTION, 
            strm.PWS_LASTSAVED, 
            strm.PWS_ORGXPATH, 
            strm.PWS_PROCESSGROUP, 
            strm.PWS_TAB, 
            strm.PWS_TOPBLOCKTITLE, 
            strm.PWS_UPDATECOUNT, 
            strm.PWS_UPDATED, 
            strm.PWS_WEBSERVICE, 
            strm.PWS_WSPRMCODE, 
            strm.PWS_WSTITLE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PWS_PROCESSGROUP,
            strm.PWS_WSPRMCODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROMPTWEBSERVICES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PWS_PROCESSGROUP=src.PWS_PROCESSGROUP) OR (target.PWS_PROCESSGROUP IS NULL AND src.PWS_PROCESSGROUP IS NULL)) AND
            ((target.PWS_WSPRMCODE=src.PWS_WSPRMCODE) OR (target.PWS_WSPRMCODE IS NULL AND src.PWS_WSPRMCODE IS NULL)) 
                when matched then update set
                    target.PWS_ACTIONCODE=src.PWS_ACTIONCODE, 
                    target.PWS_BOTTOMBLOCKTITLE=src.PWS_BOTTOMBLOCKTITLE, 
                    target.PWS_CFBLOCKTITLE=src.PWS_CFBLOCKTITLE, 
                    target.PWS_CFKEYCODE=src.PWS_CFKEYCODE, 
                    target.PWS_FUNCTION=src.PWS_FUNCTION, 
                    target.PWS_LASTSAVED=src.PWS_LASTSAVED, 
                    target.PWS_ORGXPATH=src.PWS_ORGXPATH, 
                    target.PWS_PROCESSGROUP=src.PWS_PROCESSGROUP, 
                    target.PWS_TAB=src.PWS_TAB, 
                    target.PWS_TOPBLOCKTITLE=src.PWS_TOPBLOCKTITLE, 
                    target.PWS_UPDATECOUNT=src.PWS_UPDATECOUNT, 
                    target.PWS_UPDATED=src.PWS_UPDATED, 
                    target.PWS_WEBSERVICE=src.PWS_WEBSERVICE, 
                    target.PWS_WSPRMCODE=src.PWS_WSPRMCODE, 
                    target.PWS_WSTITLE=src.PWS_WSTITLE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PWS_ACTIONCODE, PWS_BOTTOMBLOCKTITLE, PWS_CFBLOCKTITLE, PWS_CFKEYCODE, PWS_FUNCTION, PWS_LASTSAVED, PWS_ORGXPATH, PWS_PROCESSGROUP, PWS_TAB, PWS_TOPBLOCKTITLE, PWS_UPDATECOUNT, PWS_UPDATED, PWS_WEBSERVICE, PWS_WSPRMCODE, PWS_WSTITLE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PWS_ACTIONCODE, 
                    src.PWS_BOTTOMBLOCKTITLE, 
                    src.PWS_CFBLOCKTITLE, 
                    src.PWS_CFKEYCODE, 
                    src.PWS_FUNCTION, 
                    src.PWS_LASTSAVED, 
                    src.PWS_ORGXPATH, 
                    src.PWS_PROCESSGROUP, 
                    src.PWS_TAB, 
                    src.PWS_TOPBLOCKTITLE, 
                    src.PWS_UPDATECOUNT, 
                    src.PWS_UPDATED, 
                    src.PWS_WEBSERVICE, 
                    src.PWS_WSPRMCODE, 
                    src.PWS_WSTITLE, 
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