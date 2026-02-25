create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5BINS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5BINS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5BINS as target using (SELECT * FROM (SELECT 
            strm.BIN_CODE, 
            strm.BIN_CREATED, 
            strm.BIN_CREATEDBY, 
            strm.BIN_DESC, 
            strm.BIN_FORHELDITEMS, 
            strm.BIN_FORSTOCK, 
            strm.BIN_LASTSAVED, 
            strm.BIN_NOTUSED, 
            strm.BIN_STORE, 
            strm.BIN_UPDATECOUNT, 
            strm.BIN_UPDATED, 
            strm.BIN_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BIN_STORE,
            strm.BIN_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5BINS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BIN_STORE=src.BIN_STORE) OR (target.BIN_STORE IS NULL AND src.BIN_STORE IS NULL)) AND
            ((target.BIN_CODE=src.BIN_CODE) OR (target.BIN_CODE IS NULL AND src.BIN_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.BIN_CODE=src.BIN_CODE, 
                    target.BIN_CREATED=src.BIN_CREATED, 
                    target.BIN_CREATEDBY=src.BIN_CREATEDBY, 
                    target.BIN_DESC=src.BIN_DESC, 
                    target.BIN_FORHELDITEMS=src.BIN_FORHELDITEMS, 
                    target.BIN_FORSTOCK=src.BIN_FORSTOCK, 
                    target.BIN_LASTSAVED=src.BIN_LASTSAVED, 
                    target.BIN_NOTUSED=src.BIN_NOTUSED, 
                    target.BIN_STORE=src.BIN_STORE, 
                    target.BIN_UPDATECOUNT=src.BIN_UPDATECOUNT, 
                    target.BIN_UPDATED=src.BIN_UPDATED, 
                    target.BIN_UPDATEDBY=src.BIN_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    BIN_CODE, 
                    BIN_CREATED, 
                    BIN_CREATEDBY, 
                    BIN_DESC, 
                    BIN_FORHELDITEMS, 
                    BIN_FORSTOCK, 
                    BIN_LASTSAVED, 
                    BIN_NOTUSED, 
                    BIN_STORE, 
                    BIN_UPDATECOUNT, 
                    BIN_UPDATED, 
                    BIN_UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.BIN_CODE, 
                    src.BIN_CREATED, 
                    src.BIN_CREATEDBY, 
                    src.BIN_DESC, 
                    src.BIN_FORHELDITEMS, 
                    src.BIN_FORSTOCK, 
                    src.BIN_LASTSAVED, 
                    src.BIN_NOTUSED, 
                    src.BIN_STORE, 
                    src.BIN_UPDATECOUNT, 
                    src.BIN_UPDATED, 
                    src.BIN_UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5BINS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.BIN_CODE, 
            strm.BIN_CREATED, 
            strm.BIN_CREATEDBY, 
            strm.BIN_DESC, 
            strm.BIN_FORHELDITEMS, 
            strm.BIN_FORSTOCK, 
            strm.BIN_LASTSAVED, 
            strm.BIN_NOTUSED, 
            strm.BIN_STORE, 
            strm.BIN_UPDATECOUNT, 
            strm.BIN_UPDATED, 
            strm.BIN_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BIN_STORE,
            strm.BIN_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5BINS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BIN_STORE=src.BIN_STORE) OR (target.BIN_STORE IS NULL AND src.BIN_STORE IS NULL)) AND
            ((target.BIN_CODE=src.BIN_CODE) OR (target.BIN_CODE IS NULL AND src.BIN_CODE IS NULL)) 
                when matched then update set
                    target.BIN_CODE=src.BIN_CODE, 
                    target.BIN_CREATED=src.BIN_CREATED, 
                    target.BIN_CREATEDBY=src.BIN_CREATEDBY, 
                    target.BIN_DESC=src.BIN_DESC, 
                    target.BIN_FORHELDITEMS=src.BIN_FORHELDITEMS, 
                    target.BIN_FORSTOCK=src.BIN_FORSTOCK, 
                    target.BIN_LASTSAVED=src.BIN_LASTSAVED, 
                    target.BIN_NOTUSED=src.BIN_NOTUSED, 
                    target.BIN_STORE=src.BIN_STORE, 
                    target.BIN_UPDATECOUNT=src.BIN_UPDATECOUNT, 
                    target.BIN_UPDATED=src.BIN_UPDATED, 
                    target.BIN_UPDATEDBY=src.BIN_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( BIN_CODE, BIN_CREATED, BIN_CREATEDBY, BIN_DESC, BIN_FORHELDITEMS, BIN_FORSTOCK, BIN_LASTSAVED, BIN_NOTUSED, BIN_STORE, BIN_UPDATECOUNT, BIN_UPDATED, BIN_UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.BIN_CODE, 
                    src.BIN_CREATED, 
                    src.BIN_CREATEDBY, 
                    src.BIN_DESC, 
                    src.BIN_FORHELDITEMS, 
                    src.BIN_FORSTOCK, 
                    src.BIN_LASTSAVED, 
                    src.BIN_NOTUSED, 
                    src.BIN_STORE, 
                    src.BIN_UPDATECOUNT, 
                    src.BIN_UPDATED, 
                    src.BIN_UPDATEDBY, 
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