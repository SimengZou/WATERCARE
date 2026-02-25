create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPORTFIELDS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPORTFIELDS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPORTFIELDS as target using (SELECT * FROM (SELECT 
            strm.RFL_BOTNUMBER, 
            strm.RFL_DATATYPE, 
            strm.RFL_FIELD, 
            strm.RFL_FUNCTION, 
            strm.RFL_LASTSAVED, 
            strm.RFL_LINE, 
            strm.RFL_SHOWFIELD, 
            strm.RFL_UPDATECOUNT, 
            strm.RFL_UPDATED, 
            strm.RFL_WIDTH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RFL_FUNCTION,
            strm.RFL_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFIELDS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RFL_FUNCTION=src.RFL_FUNCTION) OR (target.RFL_FUNCTION IS NULL AND src.RFL_FUNCTION IS NULL)) AND
            ((target.RFL_LINE=src.RFL_LINE) OR (target.RFL_LINE IS NULL AND src.RFL_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RFL_BOTNUMBER=src.RFL_BOTNUMBER, 
                    target.RFL_DATATYPE=src.RFL_DATATYPE, 
                    target.RFL_FIELD=src.RFL_FIELD, 
                    target.RFL_FUNCTION=src.RFL_FUNCTION, 
                    target.RFL_LASTSAVED=src.RFL_LASTSAVED, 
                    target.RFL_LINE=src.RFL_LINE, 
                    target.RFL_SHOWFIELD=src.RFL_SHOWFIELD, 
                    target.RFL_UPDATECOUNT=src.RFL_UPDATECOUNT, 
                    target.RFL_UPDATED=src.RFL_UPDATED, 
                    target.RFL_WIDTH=src.RFL_WIDTH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RFL_BOTNUMBER, 
                    RFL_DATATYPE, 
                    RFL_FIELD, 
                    RFL_FUNCTION, 
                    RFL_LASTSAVED, 
                    RFL_LINE, 
                    RFL_SHOWFIELD, 
                    RFL_UPDATECOUNT, 
                    RFL_UPDATED, 
                    RFL_WIDTH, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RFL_BOTNUMBER, 
                    src.RFL_DATATYPE, 
                    src.RFL_FIELD, 
                    src.RFL_FUNCTION, 
                    src.RFL_LASTSAVED, 
                    src.RFL_LINE, 
                    src.RFL_SHOWFIELD, 
                    src.RFL_UPDATECOUNT, 
                    src.RFL_UPDATED, 
                    src.RFL_WIDTH, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPORTFIELDS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RFL_BOTNUMBER, 
            strm.RFL_DATATYPE, 
            strm.RFL_FIELD, 
            strm.RFL_FUNCTION, 
            strm.RFL_LASTSAVED, 
            strm.RFL_LINE, 
            strm.RFL_SHOWFIELD, 
            strm.RFL_UPDATECOUNT, 
            strm.RFL_UPDATED, 
            strm.RFL_WIDTH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RFL_FUNCTION,
            strm.RFL_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFIELDS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RFL_FUNCTION=src.RFL_FUNCTION) OR (target.RFL_FUNCTION IS NULL AND src.RFL_FUNCTION IS NULL)) AND
            ((target.RFL_LINE=src.RFL_LINE) OR (target.RFL_LINE IS NULL AND src.RFL_LINE IS NULL)) 
                when matched then update set
                    target.RFL_BOTNUMBER=src.RFL_BOTNUMBER, 
                    target.RFL_DATATYPE=src.RFL_DATATYPE, 
                    target.RFL_FIELD=src.RFL_FIELD, 
                    target.RFL_FUNCTION=src.RFL_FUNCTION, 
                    target.RFL_LASTSAVED=src.RFL_LASTSAVED, 
                    target.RFL_LINE=src.RFL_LINE, 
                    target.RFL_SHOWFIELD=src.RFL_SHOWFIELD, 
                    target.RFL_UPDATECOUNT=src.RFL_UPDATECOUNT, 
                    target.RFL_UPDATED=src.RFL_UPDATED, 
                    target.RFL_WIDTH=src.RFL_WIDTH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RFL_BOTNUMBER, RFL_DATATYPE, RFL_FIELD, RFL_FUNCTION, RFL_LASTSAVED, RFL_LINE, RFL_SHOWFIELD, RFL_UPDATECOUNT, RFL_UPDATED, RFL_WIDTH, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RFL_BOTNUMBER, 
                    src.RFL_DATATYPE, 
                    src.RFL_FIELD, 
                    src.RFL_FUNCTION, 
                    src.RFL_LASTSAVED, 
                    src.RFL_LINE, 
                    src.RFL_SHOWFIELD, 
                    src.RFL_UPDATECOUNT, 
                    src.RFL_UPDATED, 
                    src.RFL_WIDTH, 
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