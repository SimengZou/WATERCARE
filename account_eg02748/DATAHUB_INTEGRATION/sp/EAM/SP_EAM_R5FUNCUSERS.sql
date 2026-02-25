create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5FUNCUSERS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5FUNCUSERS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5FUNCUSERS as target using (SELECT * FROM (SELECT 
            strm.FUS_BUTFUN1, 
            strm.FUS_BUTFUN2, 
            strm.FUS_BUTFUN3, 
            strm.FUS_BUTFUN4, 
            strm.FUS_BUTFUN5, 
            strm.FUS_BUTFUN6, 
            strm.FUS_BUTICON1, 
            strm.FUS_BUTICON2, 
            strm.FUS_BUTICON3, 
            strm.FUS_BUTICON4, 
            strm.FUS_BUTICON5, 
            strm.FUS_BUTICON6, 
            strm.FUS_BUTNAME1, 
            strm.FUS_BUTNAME2, 
            strm.FUS_BUTNAME3, 
            strm.FUS_BUTNAME4, 
            strm.FUS_BUTNAME5, 
            strm.FUS_BUTNAME6, 
            strm.FUS_FILTER, 
            strm.FUS_FUNCTION, 
            strm.FUS_LASTSAVED, 
            strm.FUS_NOTES, 
            strm.FUS_PRINTER, 
            strm.FUS_SHORTN, 
            strm.FUS_UPDATECOUNT, 
            strm.FUS_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FUS_USER,
            strm.FUS_FUNCTION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUNCUSERS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FUS_USER=src.FUS_USER) OR (target.FUS_USER IS NULL AND src.FUS_USER IS NULL)) AND
            ((target.FUS_FUNCTION=src.FUS_FUNCTION) OR (target.FUS_FUNCTION IS NULL AND src.FUS_FUNCTION IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.FUS_BUTFUN1=src.FUS_BUTFUN1, 
                    target.FUS_BUTFUN2=src.FUS_BUTFUN2, 
                    target.FUS_BUTFUN3=src.FUS_BUTFUN3, 
                    target.FUS_BUTFUN4=src.FUS_BUTFUN4, 
                    target.FUS_BUTFUN5=src.FUS_BUTFUN5, 
                    target.FUS_BUTFUN6=src.FUS_BUTFUN6, 
                    target.FUS_BUTICON1=src.FUS_BUTICON1, 
                    target.FUS_BUTICON2=src.FUS_BUTICON2, 
                    target.FUS_BUTICON3=src.FUS_BUTICON3, 
                    target.FUS_BUTICON4=src.FUS_BUTICON4, 
                    target.FUS_BUTICON5=src.FUS_BUTICON5, 
                    target.FUS_BUTICON6=src.FUS_BUTICON6, 
                    target.FUS_BUTNAME1=src.FUS_BUTNAME1, 
                    target.FUS_BUTNAME2=src.FUS_BUTNAME2, 
                    target.FUS_BUTNAME3=src.FUS_BUTNAME3, 
                    target.FUS_BUTNAME4=src.FUS_BUTNAME4, 
                    target.FUS_BUTNAME5=src.FUS_BUTNAME5, 
                    target.FUS_BUTNAME6=src.FUS_BUTNAME6, 
                    target.FUS_FILTER=src.FUS_FILTER, 
                    target.FUS_FUNCTION=src.FUS_FUNCTION, 
                    target.FUS_LASTSAVED=src.FUS_LASTSAVED, 
                    target.FUS_NOTES=src.FUS_NOTES, 
                    target.FUS_PRINTER=src.FUS_PRINTER, 
                    target.FUS_SHORTN=src.FUS_SHORTN, 
                    target.FUS_UPDATECOUNT=src.FUS_UPDATECOUNT, 
                    target.FUS_USER=src.FUS_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    FUS_BUTFUN1, 
                    FUS_BUTFUN2, 
                    FUS_BUTFUN3, 
                    FUS_BUTFUN4, 
                    FUS_BUTFUN5, 
                    FUS_BUTFUN6, 
                    FUS_BUTICON1, 
                    FUS_BUTICON2, 
                    FUS_BUTICON3, 
                    FUS_BUTICON4, 
                    FUS_BUTICON5, 
                    FUS_BUTICON6, 
                    FUS_BUTNAME1, 
                    FUS_BUTNAME2, 
                    FUS_BUTNAME3, 
                    FUS_BUTNAME4, 
                    FUS_BUTNAME5, 
                    FUS_BUTNAME6, 
                    FUS_FILTER, 
                    FUS_FUNCTION, 
                    FUS_LASTSAVED, 
                    FUS_NOTES, 
                    FUS_PRINTER, 
                    FUS_SHORTN, 
                    FUS_UPDATECOUNT, 
                    FUS_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.FUS_BUTFUN1, 
                    src.FUS_BUTFUN2, 
                    src.FUS_BUTFUN3, 
                    src.FUS_BUTFUN4, 
                    src.FUS_BUTFUN5, 
                    src.FUS_BUTFUN6, 
                    src.FUS_BUTICON1, 
                    src.FUS_BUTICON2, 
                    src.FUS_BUTICON3, 
                    src.FUS_BUTICON4, 
                    src.FUS_BUTICON5, 
                    src.FUS_BUTICON6, 
                    src.FUS_BUTNAME1, 
                    src.FUS_BUTNAME2, 
                    src.FUS_BUTNAME3, 
                    src.FUS_BUTNAME4, 
                    src.FUS_BUTNAME5, 
                    src.FUS_BUTNAME6, 
                    src.FUS_FILTER, 
                    src.FUS_FUNCTION, 
                    src.FUS_LASTSAVED, 
                    src.FUS_NOTES, 
                    src.FUS_PRINTER, 
                    src.FUS_SHORTN, 
                    src.FUS_UPDATECOUNT, 
                    src.FUS_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5FUNCUSERS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.FUS_BUTFUN1, 
            strm.FUS_BUTFUN2, 
            strm.FUS_BUTFUN3, 
            strm.FUS_BUTFUN4, 
            strm.FUS_BUTFUN5, 
            strm.FUS_BUTFUN6, 
            strm.FUS_BUTICON1, 
            strm.FUS_BUTICON2, 
            strm.FUS_BUTICON3, 
            strm.FUS_BUTICON4, 
            strm.FUS_BUTICON5, 
            strm.FUS_BUTICON6, 
            strm.FUS_BUTNAME1, 
            strm.FUS_BUTNAME2, 
            strm.FUS_BUTNAME3, 
            strm.FUS_BUTNAME4, 
            strm.FUS_BUTNAME5, 
            strm.FUS_BUTNAME6, 
            strm.FUS_FILTER, 
            strm.FUS_FUNCTION, 
            strm.FUS_LASTSAVED, 
            strm.FUS_NOTES, 
            strm.FUS_PRINTER, 
            strm.FUS_SHORTN, 
            strm.FUS_UPDATECOUNT, 
            strm.FUS_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FUS_USER,
            strm.FUS_FUNCTION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUNCUSERS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FUS_USER=src.FUS_USER) OR (target.FUS_USER IS NULL AND src.FUS_USER IS NULL)) AND
            ((target.FUS_FUNCTION=src.FUS_FUNCTION) OR (target.FUS_FUNCTION IS NULL AND src.FUS_FUNCTION IS NULL)) 
                when matched then update set
                    target.FUS_BUTFUN1=src.FUS_BUTFUN1, 
                    target.FUS_BUTFUN2=src.FUS_BUTFUN2, 
                    target.FUS_BUTFUN3=src.FUS_BUTFUN3, 
                    target.FUS_BUTFUN4=src.FUS_BUTFUN4, 
                    target.FUS_BUTFUN5=src.FUS_BUTFUN5, 
                    target.FUS_BUTFUN6=src.FUS_BUTFUN6, 
                    target.FUS_BUTICON1=src.FUS_BUTICON1, 
                    target.FUS_BUTICON2=src.FUS_BUTICON2, 
                    target.FUS_BUTICON3=src.FUS_BUTICON3, 
                    target.FUS_BUTICON4=src.FUS_BUTICON4, 
                    target.FUS_BUTICON5=src.FUS_BUTICON5, 
                    target.FUS_BUTICON6=src.FUS_BUTICON6, 
                    target.FUS_BUTNAME1=src.FUS_BUTNAME1, 
                    target.FUS_BUTNAME2=src.FUS_BUTNAME2, 
                    target.FUS_BUTNAME3=src.FUS_BUTNAME3, 
                    target.FUS_BUTNAME4=src.FUS_BUTNAME4, 
                    target.FUS_BUTNAME5=src.FUS_BUTNAME5, 
                    target.FUS_BUTNAME6=src.FUS_BUTNAME6, 
                    target.FUS_FILTER=src.FUS_FILTER, 
                    target.FUS_FUNCTION=src.FUS_FUNCTION, 
                    target.FUS_LASTSAVED=src.FUS_LASTSAVED, 
                    target.FUS_NOTES=src.FUS_NOTES, 
                    target.FUS_PRINTER=src.FUS_PRINTER, 
                    target.FUS_SHORTN=src.FUS_SHORTN, 
                    target.FUS_UPDATECOUNT=src.FUS_UPDATECOUNT, 
                    target.FUS_USER=src.FUS_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( FUS_BUTFUN1, FUS_BUTFUN2, FUS_BUTFUN3, FUS_BUTFUN4, FUS_BUTFUN5, FUS_BUTFUN6, FUS_BUTICON1, FUS_BUTICON2, FUS_BUTICON3, FUS_BUTICON4, FUS_BUTICON5, FUS_BUTICON6, FUS_BUTNAME1, FUS_BUTNAME2, FUS_BUTNAME3, FUS_BUTNAME4, FUS_BUTNAME5, FUS_BUTNAME6, FUS_FILTER, FUS_FUNCTION, FUS_LASTSAVED, FUS_NOTES, FUS_PRINTER, FUS_SHORTN, FUS_UPDATECOUNT, FUS_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.FUS_BUTFUN1, 
                    src.FUS_BUTFUN2, 
                    src.FUS_BUTFUN3, 
                    src.FUS_BUTFUN4, 
                    src.FUS_BUTFUN5, 
                    src.FUS_BUTFUN6, 
                    src.FUS_BUTICON1, 
                    src.FUS_BUTICON2, 
                    src.FUS_BUTICON3, 
                    src.FUS_BUTICON4, 
                    src.FUS_BUTICON5, 
                    src.FUS_BUTICON6, 
                    src.FUS_BUTNAME1, 
                    src.FUS_BUTNAME2, 
                    src.FUS_BUTNAME3, 
                    src.FUS_BUTNAME4, 
                    src.FUS_BUTNAME5, 
                    src.FUS_BUTNAME6, 
                    src.FUS_FILTER, 
                    src.FUS_FUNCTION, 
                    src.FUS_LASTSAVED, 
                    src.FUS_NOTES, 
                    src.FUS_PRINTER, 
                    src.FUS_SHORTN, 
                    src.FUS_UPDATECOUNT, 
                    src.FUS_USER, 
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