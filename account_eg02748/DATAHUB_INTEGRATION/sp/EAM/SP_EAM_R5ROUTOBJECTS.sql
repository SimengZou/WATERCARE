create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ROUTOBJECTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ROUTOBJECTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ROUTOBJECTS as target using (SELECT * FROM (SELECT 
            strm.ROB_FROMGEOREF, 
            strm.ROB_FROMPOINT, 
            strm.ROB_FROMREFDESC, 
            strm.ROB_FROM_OFFSET, 
            strm.ROB_FROM_OFFSET_DIRECTION, 
            strm.ROB_FROM_REFERENCE, 
            strm.ROB_LASTSAVED, 
            strm.ROB_LINE, 
            strm.ROB_OBJECT, 
            strm.ROB_OBJECT_ORG, 
            strm.ROB_OBRTYPE, 
            strm.ROB_OBTYPE, 
            strm.ROB_REVISION, 
            strm.ROB_ROUTE, 
            strm.ROB_TOGEOREF, 
            strm.ROB_TOPOINT, 
            strm.ROB_TOREFDESC, 
            strm.ROB_TO_OFFSET, 
            strm.ROB_TO_OFFSET_DIRECTION, 
            strm.ROB_TO_REFERENCE, 
            strm.ROB_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROB_ROUTE,
            strm.ROB_REVISION,
            strm.ROB_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROUTOBJECTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROB_ROUTE=src.ROB_ROUTE) OR (target.ROB_ROUTE IS NULL AND src.ROB_ROUTE IS NULL)) AND
            ((target.ROB_REVISION=src.ROB_REVISION) OR (target.ROB_REVISION IS NULL AND src.ROB_REVISION IS NULL)) AND
            ((target.ROB_LINE=src.ROB_LINE) OR (target.ROB_LINE IS NULL AND src.ROB_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ROB_FROMGEOREF=src.ROB_FROMGEOREF, 
                    target.ROB_FROMPOINT=src.ROB_FROMPOINT, 
                    target.ROB_FROMREFDESC=src.ROB_FROMREFDESC, 
                    target.ROB_FROM_OFFSET=src.ROB_FROM_OFFSET, 
                    target.ROB_FROM_OFFSET_DIRECTION=src.ROB_FROM_OFFSET_DIRECTION, 
                    target.ROB_FROM_REFERENCE=src.ROB_FROM_REFERENCE, 
                    target.ROB_LASTSAVED=src.ROB_LASTSAVED, 
                    target.ROB_LINE=src.ROB_LINE, 
                    target.ROB_OBJECT=src.ROB_OBJECT, 
                    target.ROB_OBJECT_ORG=src.ROB_OBJECT_ORG, 
                    target.ROB_OBRTYPE=src.ROB_OBRTYPE, 
                    target.ROB_OBTYPE=src.ROB_OBTYPE, 
                    target.ROB_REVISION=src.ROB_REVISION, 
                    target.ROB_ROUTE=src.ROB_ROUTE, 
                    target.ROB_TOGEOREF=src.ROB_TOGEOREF, 
                    target.ROB_TOPOINT=src.ROB_TOPOINT, 
                    target.ROB_TOREFDESC=src.ROB_TOREFDESC, 
                    target.ROB_TO_OFFSET=src.ROB_TO_OFFSET, 
                    target.ROB_TO_OFFSET_DIRECTION=src.ROB_TO_OFFSET_DIRECTION, 
                    target.ROB_TO_REFERENCE=src.ROB_TO_REFERENCE, 
                    target.ROB_UPDATECOUNT=src.ROB_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ROB_FROMGEOREF, 
                    ROB_FROMPOINT, 
                    ROB_FROMREFDESC, 
                    ROB_FROM_OFFSET, 
                    ROB_FROM_OFFSET_DIRECTION, 
                    ROB_FROM_REFERENCE, 
                    ROB_LASTSAVED, 
                    ROB_LINE, 
                    ROB_OBJECT, 
                    ROB_OBJECT_ORG, 
                    ROB_OBRTYPE, 
                    ROB_OBTYPE, 
                    ROB_REVISION, 
                    ROB_ROUTE, 
                    ROB_TOGEOREF, 
                    ROB_TOPOINT, 
                    ROB_TOREFDESC, 
                    ROB_TO_OFFSET, 
                    ROB_TO_OFFSET_DIRECTION, 
                    ROB_TO_REFERENCE, 
                    ROB_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ROB_FROMGEOREF, 
                    src.ROB_FROMPOINT, 
                    src.ROB_FROMREFDESC, 
                    src.ROB_FROM_OFFSET, 
                    src.ROB_FROM_OFFSET_DIRECTION, 
                    src.ROB_FROM_REFERENCE, 
                    src.ROB_LASTSAVED, 
                    src.ROB_LINE, 
                    src.ROB_OBJECT, 
                    src.ROB_OBJECT_ORG, 
                    src.ROB_OBRTYPE, 
                    src.ROB_OBTYPE, 
                    src.ROB_REVISION, 
                    src.ROB_ROUTE, 
                    src.ROB_TOGEOREF, 
                    src.ROB_TOPOINT, 
                    src.ROB_TOREFDESC, 
                    src.ROB_TO_OFFSET, 
                    src.ROB_TO_OFFSET_DIRECTION, 
                    src.ROB_TO_REFERENCE, 
                    src.ROB_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ROUTOBJECTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ROB_FROMGEOREF, 
            strm.ROB_FROMPOINT, 
            strm.ROB_FROMREFDESC, 
            strm.ROB_FROM_OFFSET, 
            strm.ROB_FROM_OFFSET_DIRECTION, 
            strm.ROB_FROM_REFERENCE, 
            strm.ROB_LASTSAVED, 
            strm.ROB_LINE, 
            strm.ROB_OBJECT, 
            strm.ROB_OBJECT_ORG, 
            strm.ROB_OBRTYPE, 
            strm.ROB_OBTYPE, 
            strm.ROB_REVISION, 
            strm.ROB_ROUTE, 
            strm.ROB_TOGEOREF, 
            strm.ROB_TOPOINT, 
            strm.ROB_TOREFDESC, 
            strm.ROB_TO_OFFSET, 
            strm.ROB_TO_OFFSET_DIRECTION, 
            strm.ROB_TO_REFERENCE, 
            strm.ROB_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROB_ROUTE,
            strm.ROB_REVISION,
            strm.ROB_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROUTOBJECTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROB_ROUTE=src.ROB_ROUTE) OR (target.ROB_ROUTE IS NULL AND src.ROB_ROUTE IS NULL)) AND
            ((target.ROB_REVISION=src.ROB_REVISION) OR (target.ROB_REVISION IS NULL AND src.ROB_REVISION IS NULL)) AND
            ((target.ROB_LINE=src.ROB_LINE) OR (target.ROB_LINE IS NULL AND src.ROB_LINE IS NULL)) 
                when matched then update set
                    target.ROB_FROMGEOREF=src.ROB_FROMGEOREF, 
                    target.ROB_FROMPOINT=src.ROB_FROMPOINT, 
                    target.ROB_FROMREFDESC=src.ROB_FROMREFDESC, 
                    target.ROB_FROM_OFFSET=src.ROB_FROM_OFFSET, 
                    target.ROB_FROM_OFFSET_DIRECTION=src.ROB_FROM_OFFSET_DIRECTION, 
                    target.ROB_FROM_REFERENCE=src.ROB_FROM_REFERENCE, 
                    target.ROB_LASTSAVED=src.ROB_LASTSAVED, 
                    target.ROB_LINE=src.ROB_LINE, 
                    target.ROB_OBJECT=src.ROB_OBJECT, 
                    target.ROB_OBJECT_ORG=src.ROB_OBJECT_ORG, 
                    target.ROB_OBRTYPE=src.ROB_OBRTYPE, 
                    target.ROB_OBTYPE=src.ROB_OBTYPE, 
                    target.ROB_REVISION=src.ROB_REVISION, 
                    target.ROB_ROUTE=src.ROB_ROUTE, 
                    target.ROB_TOGEOREF=src.ROB_TOGEOREF, 
                    target.ROB_TOPOINT=src.ROB_TOPOINT, 
                    target.ROB_TOREFDESC=src.ROB_TOREFDESC, 
                    target.ROB_TO_OFFSET=src.ROB_TO_OFFSET, 
                    target.ROB_TO_OFFSET_DIRECTION=src.ROB_TO_OFFSET_DIRECTION, 
                    target.ROB_TO_REFERENCE=src.ROB_TO_REFERENCE, 
                    target.ROB_UPDATECOUNT=src.ROB_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ROB_FROMGEOREF, ROB_FROMPOINT, ROB_FROMREFDESC, ROB_FROM_OFFSET, ROB_FROM_OFFSET_DIRECTION, ROB_FROM_REFERENCE, ROB_LASTSAVED, ROB_LINE, ROB_OBJECT, ROB_OBJECT_ORG, ROB_OBRTYPE, ROB_OBTYPE, ROB_REVISION, ROB_ROUTE, ROB_TOGEOREF, ROB_TOPOINT, ROB_TOREFDESC, ROB_TO_OFFSET, ROB_TO_OFFSET_DIRECTION, ROB_TO_REFERENCE, ROB_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ROB_FROMGEOREF, 
                    src.ROB_FROMPOINT, 
                    src.ROB_FROMREFDESC, 
                    src.ROB_FROM_OFFSET, 
                    src.ROB_FROM_OFFSET_DIRECTION, 
                    src.ROB_FROM_REFERENCE, 
                    src.ROB_LASTSAVED, 
                    src.ROB_LINE, 
                    src.ROB_OBJECT, 
                    src.ROB_OBJECT_ORG, 
                    src.ROB_OBRTYPE, 
                    src.ROB_OBTYPE, 
                    src.ROB_REVISION, 
                    src.ROB_ROUTE, 
                    src.ROB_TOGEOREF, 
                    src.ROB_TOPOINT, 
                    src.ROB_TOREFDESC, 
                    src.ROB_TO_OFFSET, 
                    src.ROB_TO_OFFSET_DIRECTION, 
                    src.ROB_TO_REFERENCE, 
                    src.ROB_UPDATECOUNT, 
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