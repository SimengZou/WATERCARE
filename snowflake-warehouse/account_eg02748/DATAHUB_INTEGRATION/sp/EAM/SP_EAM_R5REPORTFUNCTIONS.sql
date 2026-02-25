create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPORTFUNCTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPORTFUNCTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPORTFUNCTIONS as target using (SELECT * FROM (SELECT 
            strm.RFN_FIELDORDER, 
            strm.RFN_FROMCLAUSE, 
            strm.RFN_FUNCTION, 
            strm.RFN_GROUPBY, 
            strm.RFN_LASTSAVED, 
            strm.RFN_ORDERBY, 
            strm.RFN_ORDERTYPE, 
            strm.RFN_UPDATECOUNT, 
            strm.RFN_UPDATED, 
            strm.RFN_WHERECLAUSE1, 
            strm.RFN_WHERECLAUSE2, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RFN_FUNCTION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFUNCTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RFN_FUNCTION=src.RFN_FUNCTION) OR (target.RFN_FUNCTION IS NULL AND src.RFN_FUNCTION IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RFN_FIELDORDER=src.RFN_FIELDORDER, 
                    target.RFN_FROMCLAUSE=src.RFN_FROMCLAUSE, 
                    target.RFN_FUNCTION=src.RFN_FUNCTION, 
                    target.RFN_GROUPBY=src.RFN_GROUPBY, 
                    target.RFN_LASTSAVED=src.RFN_LASTSAVED, 
                    target.RFN_ORDERBY=src.RFN_ORDERBY, 
                    target.RFN_ORDERTYPE=src.RFN_ORDERTYPE, 
                    target.RFN_UPDATECOUNT=src.RFN_UPDATECOUNT, 
                    target.RFN_UPDATED=src.RFN_UPDATED, 
                    target.RFN_WHERECLAUSE1=src.RFN_WHERECLAUSE1, 
                    target.RFN_WHERECLAUSE2=src.RFN_WHERECLAUSE2, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RFN_FIELDORDER, 
                    RFN_FROMCLAUSE, 
                    RFN_FUNCTION, 
                    RFN_GROUPBY, 
                    RFN_LASTSAVED, 
                    RFN_ORDERBY, 
                    RFN_ORDERTYPE, 
                    RFN_UPDATECOUNT, 
                    RFN_UPDATED, 
                    RFN_WHERECLAUSE1, 
                    RFN_WHERECLAUSE2, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RFN_FIELDORDER, 
                    src.RFN_FROMCLAUSE, 
                    src.RFN_FUNCTION, 
                    src.RFN_GROUPBY, 
                    src.RFN_LASTSAVED, 
                    src.RFN_ORDERBY, 
                    src.RFN_ORDERTYPE, 
                    src.RFN_UPDATECOUNT, 
                    src.RFN_UPDATED, 
                    src.RFN_WHERECLAUSE1, 
                    src.RFN_WHERECLAUSE2, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPORTFUNCTIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RFN_FIELDORDER, 
            strm.RFN_FROMCLAUSE, 
            strm.RFN_FUNCTION, 
            strm.RFN_GROUPBY, 
            strm.RFN_LASTSAVED, 
            strm.RFN_ORDERBY, 
            strm.RFN_ORDERTYPE, 
            strm.RFN_UPDATECOUNT, 
            strm.RFN_UPDATED, 
            strm.RFN_WHERECLAUSE1, 
            strm.RFN_WHERECLAUSE2, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RFN_FUNCTION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFUNCTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RFN_FUNCTION=src.RFN_FUNCTION) OR (target.RFN_FUNCTION IS NULL AND src.RFN_FUNCTION IS NULL)) 
                when matched then update set
                    target.RFN_FIELDORDER=src.RFN_FIELDORDER, 
                    target.RFN_FROMCLAUSE=src.RFN_FROMCLAUSE, 
                    target.RFN_FUNCTION=src.RFN_FUNCTION, 
                    target.RFN_GROUPBY=src.RFN_GROUPBY, 
                    target.RFN_LASTSAVED=src.RFN_LASTSAVED, 
                    target.RFN_ORDERBY=src.RFN_ORDERBY, 
                    target.RFN_ORDERTYPE=src.RFN_ORDERTYPE, 
                    target.RFN_UPDATECOUNT=src.RFN_UPDATECOUNT, 
                    target.RFN_UPDATED=src.RFN_UPDATED, 
                    target.RFN_WHERECLAUSE1=src.RFN_WHERECLAUSE1, 
                    target.RFN_WHERECLAUSE2=src.RFN_WHERECLAUSE2, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RFN_FIELDORDER, RFN_FROMCLAUSE, RFN_FUNCTION, RFN_GROUPBY, RFN_LASTSAVED, RFN_ORDERBY, RFN_ORDERTYPE, RFN_UPDATECOUNT, RFN_UPDATED, RFN_WHERECLAUSE1, RFN_WHERECLAUSE2, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RFN_FIELDORDER, 
                    src.RFN_FROMCLAUSE, 
                    src.RFN_FUNCTION, 
                    src.RFN_GROUPBY, 
                    src.RFN_LASTSAVED, 
                    src.RFN_ORDERBY, 
                    src.RFN_ORDERTYPE, 
                    src.RFN_UPDATECOUNT, 
                    src.RFN_UPDATED, 
                    src.RFN_WHERECLAUSE1, 
                    src.RFN_WHERECLAUSE2, 
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