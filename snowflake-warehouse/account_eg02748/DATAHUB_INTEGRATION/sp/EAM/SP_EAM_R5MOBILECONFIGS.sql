create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MOBILECONFIGS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MOBILECONFIGS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MOBILECONFIGS as target using (SELECT * FROM (SELECT 
            strm.MCF_CODE, 
            strm.MCF_COMPTYPE, 
            strm.MCF_CONFIG, 
            strm.MCF_CREATED, 
            strm.MCF_DESK, 
            strm.MCF_GROUP, 
            strm.MCF_LASTSAVED, 
            strm.MCF_RCODE, 
            strm.MCF_UPDATECOUNT, 
            strm.MCF_UPDATED, 
            strm.MCF_USER, 
            strm.MCF_XMLCONFIG, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MCF_CODE,
            strm.MCF_RCODE,
            strm.MCF_CONFIG,
            strm.MCF_DESK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILECONFIGS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MCF_CODE=src.MCF_CODE) OR (target.MCF_CODE IS NULL AND src.MCF_CODE IS NULL)) AND
            ((target.MCF_RCODE=src.MCF_RCODE) OR (target.MCF_RCODE IS NULL AND src.MCF_RCODE IS NULL)) AND
            ((target.MCF_CONFIG=src.MCF_CONFIG) OR (target.MCF_CONFIG IS NULL AND src.MCF_CONFIG IS NULL)) AND
            ((target.MCF_DESK=src.MCF_DESK) OR (target.MCF_DESK IS NULL AND src.MCF_DESK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MCF_CODE=src.MCF_CODE, 
                    target.MCF_COMPTYPE=src.MCF_COMPTYPE, 
                    target.MCF_CONFIG=src.MCF_CONFIG, 
                    target.MCF_CREATED=src.MCF_CREATED, 
                    target.MCF_DESK=src.MCF_DESK, 
                    target.MCF_GROUP=src.MCF_GROUP, 
                    target.MCF_LASTSAVED=src.MCF_LASTSAVED, 
                    target.MCF_RCODE=src.MCF_RCODE, 
                    target.MCF_UPDATECOUNT=src.MCF_UPDATECOUNT, 
                    target.MCF_UPDATED=src.MCF_UPDATED, 
                    target.MCF_USER=src.MCF_USER, 
                    target.MCF_XMLCONFIG=src.MCF_XMLCONFIG, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MCF_CODE, 
                    MCF_COMPTYPE, 
                    MCF_CONFIG, 
                    MCF_CREATED, 
                    MCF_DESK, 
                    MCF_GROUP, 
                    MCF_LASTSAVED, 
                    MCF_RCODE, 
                    MCF_UPDATECOUNT, 
                    MCF_UPDATED, 
                    MCF_USER, 
                    MCF_XMLCONFIG, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MCF_CODE, 
                    src.MCF_COMPTYPE, 
                    src.MCF_CONFIG, 
                    src.MCF_CREATED, 
                    src.MCF_DESK, 
                    src.MCF_GROUP, 
                    src.MCF_LASTSAVED, 
                    src.MCF_RCODE, 
                    src.MCF_UPDATECOUNT, 
                    src.MCF_UPDATED, 
                    src.MCF_USER, 
                    src.MCF_XMLCONFIG, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MOBILECONFIGS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MCF_CODE, 
            strm.MCF_COMPTYPE, 
            strm.MCF_CONFIG, 
            strm.MCF_CREATED, 
            strm.MCF_DESK, 
            strm.MCF_GROUP, 
            strm.MCF_LASTSAVED, 
            strm.MCF_RCODE, 
            strm.MCF_UPDATECOUNT, 
            strm.MCF_UPDATED, 
            strm.MCF_USER, 
            strm.MCF_XMLCONFIG, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MCF_CODE,
            strm.MCF_RCODE,
            strm.MCF_CONFIG,
            strm.MCF_DESK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILECONFIGS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MCF_CODE=src.MCF_CODE) OR (target.MCF_CODE IS NULL AND src.MCF_CODE IS NULL)) AND
            ((target.MCF_RCODE=src.MCF_RCODE) OR (target.MCF_RCODE IS NULL AND src.MCF_RCODE IS NULL)) AND
            ((target.MCF_CONFIG=src.MCF_CONFIG) OR (target.MCF_CONFIG IS NULL AND src.MCF_CONFIG IS NULL)) AND
            ((target.MCF_DESK=src.MCF_DESK) OR (target.MCF_DESK IS NULL AND src.MCF_DESK IS NULL)) 
                when matched then update set
                    target.MCF_CODE=src.MCF_CODE, 
                    target.MCF_COMPTYPE=src.MCF_COMPTYPE, 
                    target.MCF_CONFIG=src.MCF_CONFIG, 
                    target.MCF_CREATED=src.MCF_CREATED, 
                    target.MCF_DESK=src.MCF_DESK, 
                    target.MCF_GROUP=src.MCF_GROUP, 
                    target.MCF_LASTSAVED=src.MCF_LASTSAVED, 
                    target.MCF_RCODE=src.MCF_RCODE, 
                    target.MCF_UPDATECOUNT=src.MCF_UPDATECOUNT, 
                    target.MCF_UPDATED=src.MCF_UPDATED, 
                    target.MCF_USER=src.MCF_USER, 
                    target.MCF_XMLCONFIG=src.MCF_XMLCONFIG, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MCF_CODE, MCF_COMPTYPE, MCF_CONFIG, MCF_CREATED, MCF_DESK, MCF_GROUP, MCF_LASTSAVED, MCF_RCODE, MCF_UPDATECOUNT, MCF_UPDATED, MCF_USER, MCF_XMLCONFIG, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MCF_CODE, 
                    src.MCF_COMPTYPE, 
                    src.MCF_CONFIG, 
                    src.MCF_CREATED, 
                    src.MCF_DESK, 
                    src.MCF_GROUP, 
                    src.MCF_LASTSAVED, 
                    src.MCF_RCODE, 
                    src.MCF_UPDATECOUNT, 
                    src.MCF_UPDATED, 
                    src.MCF_USER, 
                    src.MCF_XMLCONFIG, 
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