create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPORTSUBTOTAL()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPORTSUBTOTAL_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPORTSUBTOTAL as target using (SELECT * FROM (SELECT 
            strm.RST_BOTNUMBER, 
            strm.RST_DATATYPE, 
            strm.RST_FIELD, 
            strm.RST_FUNCTION, 
            strm.RST_GROUPLINE, 
            strm.RST_LASTSAVED, 
            strm.RST_LINE, 
            strm.RST_UPDATECOUNT, 
            strm.RST_UPDATED, 
            strm.RST_WIDTH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RST_FUNCTION,
            strm.RST_GROUPLINE,
            strm.RST_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTSUBTOTAL as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RST_FUNCTION=src.RST_FUNCTION) OR (target.RST_FUNCTION IS NULL AND src.RST_FUNCTION IS NULL)) AND
            ((target.RST_GROUPLINE=src.RST_GROUPLINE) OR (target.RST_GROUPLINE IS NULL AND src.RST_GROUPLINE IS NULL)) AND
            ((target.RST_LINE=src.RST_LINE) OR (target.RST_LINE IS NULL AND src.RST_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RST_BOTNUMBER=src.RST_BOTNUMBER, 
                    target.RST_DATATYPE=src.RST_DATATYPE, 
                    target.RST_FIELD=src.RST_FIELD, 
                    target.RST_FUNCTION=src.RST_FUNCTION, 
                    target.RST_GROUPLINE=src.RST_GROUPLINE, 
                    target.RST_LASTSAVED=src.RST_LASTSAVED, 
                    target.RST_LINE=src.RST_LINE, 
                    target.RST_UPDATECOUNT=src.RST_UPDATECOUNT, 
                    target.RST_UPDATED=src.RST_UPDATED, 
                    target.RST_WIDTH=src.RST_WIDTH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RST_BOTNUMBER, 
                    RST_DATATYPE, 
                    RST_FIELD, 
                    RST_FUNCTION, 
                    RST_GROUPLINE, 
                    RST_LASTSAVED, 
                    RST_LINE, 
                    RST_UPDATECOUNT, 
                    RST_UPDATED, 
                    RST_WIDTH, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RST_BOTNUMBER, 
                    src.RST_DATATYPE, 
                    src.RST_FIELD, 
                    src.RST_FUNCTION, 
                    src.RST_GROUPLINE, 
                    src.RST_LASTSAVED, 
                    src.RST_LINE, 
                    src.RST_UPDATECOUNT, 
                    src.RST_UPDATED, 
                    src.RST_WIDTH, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPORTSUBTOTAL_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RST_BOTNUMBER, 
            strm.RST_DATATYPE, 
            strm.RST_FIELD, 
            strm.RST_FUNCTION, 
            strm.RST_GROUPLINE, 
            strm.RST_LASTSAVED, 
            strm.RST_LINE, 
            strm.RST_UPDATECOUNT, 
            strm.RST_UPDATED, 
            strm.RST_WIDTH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RST_FUNCTION,
            strm.RST_GROUPLINE,
            strm.RST_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTSUBTOTAL as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RST_FUNCTION=src.RST_FUNCTION) OR (target.RST_FUNCTION IS NULL AND src.RST_FUNCTION IS NULL)) AND
            ((target.RST_GROUPLINE=src.RST_GROUPLINE) OR (target.RST_GROUPLINE IS NULL AND src.RST_GROUPLINE IS NULL)) AND
            ((target.RST_LINE=src.RST_LINE) OR (target.RST_LINE IS NULL AND src.RST_LINE IS NULL)) 
                when matched then update set
                    target.RST_BOTNUMBER=src.RST_BOTNUMBER, 
                    target.RST_DATATYPE=src.RST_DATATYPE, 
                    target.RST_FIELD=src.RST_FIELD, 
                    target.RST_FUNCTION=src.RST_FUNCTION, 
                    target.RST_GROUPLINE=src.RST_GROUPLINE, 
                    target.RST_LASTSAVED=src.RST_LASTSAVED, 
                    target.RST_LINE=src.RST_LINE, 
                    target.RST_UPDATECOUNT=src.RST_UPDATECOUNT, 
                    target.RST_UPDATED=src.RST_UPDATED, 
                    target.RST_WIDTH=src.RST_WIDTH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RST_BOTNUMBER, RST_DATATYPE, RST_FIELD, RST_FUNCTION, RST_GROUPLINE, RST_LASTSAVED, RST_LINE, RST_UPDATECOUNT, RST_UPDATED, RST_WIDTH, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RST_BOTNUMBER, 
                    src.RST_DATATYPE, 
                    src.RST_FIELD, 
                    src.RST_FUNCTION, 
                    src.RST_GROUPLINE, 
                    src.RST_LASTSAVED, 
                    src.RST_LINE, 
                    src.RST_UPDATECOUNT, 
                    src.RST_UPDATED, 
                    src.RST_WIDTH, 
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