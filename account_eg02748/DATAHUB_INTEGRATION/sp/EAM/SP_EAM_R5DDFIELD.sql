create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DDFIELD()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DDFIELD_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DDFIELD as target using (SELECT * FROM (SELECT 
            strm.DDF_DATATYPE, 
            strm.DDF_DESC, 
            strm.DDF_FIELDID, 
            strm.DDF_LASTSAVED, 
            strm.DDF_LVGRID, 
            strm.DDF_RENTITY, 
            strm.DDF_SOURCENAME, 
            strm.DDF_TABLENAME, 
            strm.DDF_UPDATECOUNT, 
            strm.DDF_VALUEMAPID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DDF_FIELDID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DDFIELD as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DDF_FIELDID=src.DDF_FIELDID) OR (target.DDF_FIELDID IS NULL AND src.DDF_FIELDID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DDF_DATATYPE=src.DDF_DATATYPE, 
                    target.DDF_DESC=src.DDF_DESC, 
                    target.DDF_FIELDID=src.DDF_FIELDID, 
                    target.DDF_LASTSAVED=src.DDF_LASTSAVED, 
                    target.DDF_LVGRID=src.DDF_LVGRID, 
                    target.DDF_RENTITY=src.DDF_RENTITY, 
                    target.DDF_SOURCENAME=src.DDF_SOURCENAME, 
                    target.DDF_TABLENAME=src.DDF_TABLENAME, 
                    target.DDF_UPDATECOUNT=src.DDF_UPDATECOUNT, 
                    target.DDF_VALUEMAPID=src.DDF_VALUEMAPID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DDF_DATATYPE, 
                    DDF_DESC, 
                    DDF_FIELDID, 
                    DDF_LASTSAVED, 
                    DDF_LVGRID, 
                    DDF_RENTITY, 
                    DDF_SOURCENAME, 
                    DDF_TABLENAME, 
                    DDF_UPDATECOUNT, 
                    DDF_VALUEMAPID, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DDF_DATATYPE, 
                    src.DDF_DESC, 
                    src.DDF_FIELDID, 
                    src.DDF_LASTSAVED, 
                    src.DDF_LVGRID, 
                    src.DDF_RENTITY, 
                    src.DDF_SOURCENAME, 
                    src.DDF_TABLENAME, 
                    src.DDF_UPDATECOUNT, 
                    src.DDF_VALUEMAPID, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DDFIELD_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DDF_DATATYPE, 
            strm.DDF_DESC, 
            strm.DDF_FIELDID, 
            strm.DDF_LASTSAVED, 
            strm.DDF_LVGRID, 
            strm.DDF_RENTITY, 
            strm.DDF_SOURCENAME, 
            strm.DDF_TABLENAME, 
            strm.DDF_UPDATECOUNT, 
            strm.DDF_VALUEMAPID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DDF_FIELDID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DDFIELD as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DDF_FIELDID=src.DDF_FIELDID) OR (target.DDF_FIELDID IS NULL AND src.DDF_FIELDID IS NULL)) 
                when matched then update set
                    target.DDF_DATATYPE=src.DDF_DATATYPE, 
                    target.DDF_DESC=src.DDF_DESC, 
                    target.DDF_FIELDID=src.DDF_FIELDID, 
                    target.DDF_LASTSAVED=src.DDF_LASTSAVED, 
                    target.DDF_LVGRID=src.DDF_LVGRID, 
                    target.DDF_RENTITY=src.DDF_RENTITY, 
                    target.DDF_SOURCENAME=src.DDF_SOURCENAME, 
                    target.DDF_TABLENAME=src.DDF_TABLENAME, 
                    target.DDF_UPDATECOUNT=src.DDF_UPDATECOUNT, 
                    target.DDF_VALUEMAPID=src.DDF_VALUEMAPID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DDF_DATATYPE, DDF_DESC, DDF_FIELDID, DDF_LASTSAVED, DDF_LVGRID, DDF_RENTITY, DDF_SOURCENAME, DDF_TABLENAME, DDF_UPDATECOUNT, DDF_VALUEMAPID, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DDF_DATATYPE, 
                    src.DDF_DESC, 
                    src.DDF_FIELDID, 
                    src.DDF_LASTSAVED, 
                    src.DDF_LVGRID, 
                    src.DDF_RENTITY, 
                    src.DDF_SOURCENAME, 
                    src.DDF_TABLENAME, 
                    src.DDF_UPDATECOUNT, 
                    src.DDF_VALUEMAPID, 
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