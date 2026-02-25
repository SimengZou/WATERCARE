create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PMPLANSESSION()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PMPLANSESSION_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PMPLANSESSION as target using (SELECT * FROM (SELECT 
            strm.PPS_DUEDATE, 
            strm.PPS_DUEDAYOFWEEK, 
            strm.PPS_DUEWEEKOFMONTH, 
            strm.PPS_IGNOREFREQWARNING, 
            strm.PPS_IGNORERANGEWARNING, 
            strm.PPS_LASTSAVED, 
            strm.PPS_LOCKED, 
            strm.PPS_NESTINGREF, 
            strm.PPS_PK, 
            strm.PPS_PMREVISION, 
            strm.PPS_PPOPK, 
            strm.PPS_SESSIONID, 
            strm.PPS_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PPS_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMPLANSESSION as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PPS_PK=src.PPS_PK) OR (target.PPS_PK IS NULL AND src.PPS_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PPS_DUEDATE=src.PPS_DUEDATE, 
                    target.PPS_DUEDAYOFWEEK=src.PPS_DUEDAYOFWEEK, 
                    target.PPS_DUEWEEKOFMONTH=src.PPS_DUEWEEKOFMONTH, 
                    target.PPS_IGNOREFREQWARNING=src.PPS_IGNOREFREQWARNING, 
                    target.PPS_IGNORERANGEWARNING=src.PPS_IGNORERANGEWARNING, 
                    target.PPS_LASTSAVED=src.PPS_LASTSAVED, 
                    target.PPS_LOCKED=src.PPS_LOCKED, 
                    target.PPS_NESTINGREF=src.PPS_NESTINGREF, 
                    target.PPS_PK=src.PPS_PK, 
                    target.PPS_PMREVISION=src.PPS_PMREVISION, 
                    target.PPS_PPOPK=src.PPS_PPOPK, 
                    target.PPS_SESSIONID=src.PPS_SESSIONID, 
                    target.PPS_UPDATECOUNT=src.PPS_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PPS_DUEDATE, 
                    PPS_DUEDAYOFWEEK, 
                    PPS_DUEWEEKOFMONTH, 
                    PPS_IGNOREFREQWARNING, 
                    PPS_IGNORERANGEWARNING, 
                    PPS_LASTSAVED, 
                    PPS_LOCKED, 
                    PPS_NESTINGREF, 
                    PPS_PK, 
                    PPS_PMREVISION, 
                    PPS_PPOPK, 
                    PPS_SESSIONID, 
                    PPS_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PPS_DUEDATE, 
                    src.PPS_DUEDAYOFWEEK, 
                    src.PPS_DUEWEEKOFMONTH, 
                    src.PPS_IGNOREFREQWARNING, 
                    src.PPS_IGNORERANGEWARNING, 
                    src.PPS_LASTSAVED, 
                    src.PPS_LOCKED, 
                    src.PPS_NESTINGREF, 
                    src.PPS_PK, 
                    src.PPS_PMREVISION, 
                    src.PPS_PPOPK, 
                    src.PPS_SESSIONID, 
                    src.PPS_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PMPLANSESSION_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PPS_DUEDATE, 
            strm.PPS_DUEDAYOFWEEK, 
            strm.PPS_DUEWEEKOFMONTH, 
            strm.PPS_IGNOREFREQWARNING, 
            strm.PPS_IGNORERANGEWARNING, 
            strm.PPS_LASTSAVED, 
            strm.PPS_LOCKED, 
            strm.PPS_NESTINGREF, 
            strm.PPS_PK, 
            strm.PPS_PMREVISION, 
            strm.PPS_PPOPK, 
            strm.PPS_SESSIONID, 
            strm.PPS_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PPS_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMPLANSESSION as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PPS_PK=src.PPS_PK) OR (target.PPS_PK IS NULL AND src.PPS_PK IS NULL)) 
                when matched then update set
                    target.PPS_DUEDATE=src.PPS_DUEDATE, 
                    target.PPS_DUEDAYOFWEEK=src.PPS_DUEDAYOFWEEK, 
                    target.PPS_DUEWEEKOFMONTH=src.PPS_DUEWEEKOFMONTH, 
                    target.PPS_IGNOREFREQWARNING=src.PPS_IGNOREFREQWARNING, 
                    target.PPS_IGNORERANGEWARNING=src.PPS_IGNORERANGEWARNING, 
                    target.PPS_LASTSAVED=src.PPS_LASTSAVED, 
                    target.PPS_LOCKED=src.PPS_LOCKED, 
                    target.PPS_NESTINGREF=src.PPS_NESTINGREF, 
                    target.PPS_PK=src.PPS_PK, 
                    target.PPS_PMREVISION=src.PPS_PMREVISION, 
                    target.PPS_PPOPK=src.PPS_PPOPK, 
                    target.PPS_SESSIONID=src.PPS_SESSIONID, 
                    target.PPS_UPDATECOUNT=src.PPS_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PPS_DUEDATE, PPS_DUEDAYOFWEEK, PPS_DUEWEEKOFMONTH, PPS_IGNOREFREQWARNING, PPS_IGNORERANGEWARNING, PPS_LASTSAVED, PPS_LOCKED, PPS_NESTINGREF, PPS_PK, PPS_PMREVISION, PPS_PPOPK, PPS_SESSIONID, PPS_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PPS_DUEDATE, 
                    src.PPS_DUEDAYOFWEEK, 
                    src.PPS_DUEWEEKOFMONTH, 
                    src.PPS_IGNOREFREQWARNING, 
                    src.PPS_IGNORERANGEWARNING, 
                    src.PPS_LASTSAVED, 
                    src.PPS_LOCKED, 
                    src.PPS_NESTINGREF, 
                    src.PPS_PK, 
                    src.PPS_PMREVISION, 
                    src.PPS_PPOPK, 
                    src.PPS_SESSIONID, 
                    src.PPS_UPDATECOUNT, 
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