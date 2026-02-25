create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MOBILEQUERIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MOBILEQUERIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MOBILEQUERIES as target using (SELECT * FROM (SELECT 
            strm.MQR_COLUMNMAP, 
            strm.MQR_CREATETABLE, 
            strm.MQR_GRIDNAME, 
            strm.MQR_LASTSAVED, 
            strm.MQR_PREPAREGRID, 
            strm.MQR_PREPARETABLEUSED, 
            strm.MQR_SINGLETHREADPERCONN, 
            strm.MQR_SQLTEXT, 
            strm.MQR_TABLENAME, 
            strm.MQR_UPDATECOUNT, 
            strm.MQR_UPDATED, 
            strm.MQR_VERSION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MQR_GRIDNAME,
            strm.MQR_TABLENAME,
            strm.MQR_VERSION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEQUERIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MQR_GRIDNAME=src.MQR_GRIDNAME) OR (target.MQR_GRIDNAME IS NULL AND src.MQR_GRIDNAME IS NULL)) AND
            ((target.MQR_TABLENAME=src.MQR_TABLENAME) OR (target.MQR_TABLENAME IS NULL AND src.MQR_TABLENAME IS NULL)) AND
            ((target.MQR_VERSION=src.MQR_VERSION) OR (target.MQR_VERSION IS NULL AND src.MQR_VERSION IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MQR_COLUMNMAP=src.MQR_COLUMNMAP, 
                    target.MQR_CREATETABLE=src.MQR_CREATETABLE, 
                    target.MQR_GRIDNAME=src.MQR_GRIDNAME, 
                    target.MQR_LASTSAVED=src.MQR_LASTSAVED, 
                    target.MQR_PREPAREGRID=src.MQR_PREPAREGRID, 
                    target.MQR_PREPARETABLEUSED=src.MQR_PREPARETABLEUSED, 
                    target.MQR_SINGLETHREADPERCONN=src.MQR_SINGLETHREADPERCONN, 
                    target.MQR_SQLTEXT=src.MQR_SQLTEXT, 
                    target.MQR_TABLENAME=src.MQR_TABLENAME, 
                    target.MQR_UPDATECOUNT=src.MQR_UPDATECOUNT, 
                    target.MQR_UPDATED=src.MQR_UPDATED, 
                    target.MQR_VERSION=src.MQR_VERSION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MQR_COLUMNMAP, 
                    MQR_CREATETABLE, 
                    MQR_GRIDNAME, 
                    MQR_LASTSAVED, 
                    MQR_PREPAREGRID, 
                    MQR_PREPARETABLEUSED, 
                    MQR_SINGLETHREADPERCONN, 
                    MQR_SQLTEXT, 
                    MQR_TABLENAME, 
                    MQR_UPDATECOUNT, 
                    MQR_UPDATED, 
                    MQR_VERSION, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MQR_COLUMNMAP, 
                    src.MQR_CREATETABLE, 
                    src.MQR_GRIDNAME, 
                    src.MQR_LASTSAVED, 
                    src.MQR_PREPAREGRID, 
                    src.MQR_PREPARETABLEUSED, 
                    src.MQR_SINGLETHREADPERCONN, 
                    src.MQR_SQLTEXT, 
                    src.MQR_TABLENAME, 
                    src.MQR_UPDATECOUNT, 
                    src.MQR_UPDATED, 
                    src.MQR_VERSION, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MOBILEQUERIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MQR_COLUMNMAP, 
            strm.MQR_CREATETABLE, 
            strm.MQR_GRIDNAME, 
            strm.MQR_LASTSAVED, 
            strm.MQR_PREPAREGRID, 
            strm.MQR_PREPARETABLEUSED, 
            strm.MQR_SINGLETHREADPERCONN, 
            strm.MQR_SQLTEXT, 
            strm.MQR_TABLENAME, 
            strm.MQR_UPDATECOUNT, 
            strm.MQR_UPDATED, 
            strm.MQR_VERSION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MQR_GRIDNAME,
            strm.MQR_TABLENAME,
            strm.MQR_VERSION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEQUERIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MQR_GRIDNAME=src.MQR_GRIDNAME) OR (target.MQR_GRIDNAME IS NULL AND src.MQR_GRIDNAME IS NULL)) AND
            ((target.MQR_TABLENAME=src.MQR_TABLENAME) OR (target.MQR_TABLENAME IS NULL AND src.MQR_TABLENAME IS NULL)) AND
            ((target.MQR_VERSION=src.MQR_VERSION) OR (target.MQR_VERSION IS NULL AND src.MQR_VERSION IS NULL)) 
                when matched then update set
                    target.MQR_COLUMNMAP=src.MQR_COLUMNMAP, 
                    target.MQR_CREATETABLE=src.MQR_CREATETABLE, 
                    target.MQR_GRIDNAME=src.MQR_GRIDNAME, 
                    target.MQR_LASTSAVED=src.MQR_LASTSAVED, 
                    target.MQR_PREPAREGRID=src.MQR_PREPAREGRID, 
                    target.MQR_PREPARETABLEUSED=src.MQR_PREPARETABLEUSED, 
                    target.MQR_SINGLETHREADPERCONN=src.MQR_SINGLETHREADPERCONN, 
                    target.MQR_SQLTEXT=src.MQR_SQLTEXT, 
                    target.MQR_TABLENAME=src.MQR_TABLENAME, 
                    target.MQR_UPDATECOUNT=src.MQR_UPDATECOUNT, 
                    target.MQR_UPDATED=src.MQR_UPDATED, 
                    target.MQR_VERSION=src.MQR_VERSION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MQR_COLUMNMAP, MQR_CREATETABLE, MQR_GRIDNAME, MQR_LASTSAVED, MQR_PREPAREGRID, MQR_PREPARETABLEUSED, MQR_SINGLETHREADPERCONN, MQR_SQLTEXT, MQR_TABLENAME, MQR_UPDATECOUNT, MQR_UPDATED, MQR_VERSION, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MQR_COLUMNMAP, 
                    src.MQR_CREATETABLE, 
                    src.MQR_GRIDNAME, 
                    src.MQR_LASTSAVED, 
                    src.MQR_PREPAREGRID, 
                    src.MQR_PREPARETABLEUSED, 
                    src.MQR_SINGLETHREADPERCONN, 
                    src.MQR_SQLTEXT, 
                    src.MQR_TABLENAME, 
                    src.MQR_UPDATECOUNT, 
                    src.MQR_UPDATED, 
                    src.MQR_VERSION, 
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