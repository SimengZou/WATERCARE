create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5EVENTCOST()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5EVENTCOST_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5EVENTCOST as target using (SELECT * FROM (SELECT 
            strm.EVO_COSTCALCULATED, 
            strm.EVO_DIRECTMATERIAL, 
            strm.EVO_EVENT, 
            strm.EVO_HIREDLABOR, 
            strm.EVO_HOURS, 
            strm.EVO_LABOR, 
            strm.EVO_LASTSAVED, 
            strm.EVO_MATERIAL, 
            strm.EVO_OWNLABOR, 
            strm.EVO_RECALCCOST, 
            strm.EVO_SERVICESLABOR, 
            strm.EVO_STOCKMATERIAL, 
            strm.EVO_TOOL, 
            strm.EVO_TOTAL, 
            strm.EVO_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EVO_EVENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTCOST as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EVO_EVENT=src.EVO_EVENT) OR (target.EVO_EVENT IS NULL AND src.EVO_EVENT IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.EVO_COSTCALCULATED=src.EVO_COSTCALCULATED, 
                    target.EVO_DIRECTMATERIAL=src.EVO_DIRECTMATERIAL, 
                    target.EVO_EVENT=src.EVO_EVENT, 
                    target.EVO_HIREDLABOR=src.EVO_HIREDLABOR, 
                    target.EVO_HOURS=src.EVO_HOURS, 
                    target.EVO_LABOR=src.EVO_LABOR, 
                    target.EVO_LASTSAVED=src.EVO_LASTSAVED, 
                    target.EVO_MATERIAL=src.EVO_MATERIAL, 
                    target.EVO_OWNLABOR=src.EVO_OWNLABOR, 
                    target.EVO_RECALCCOST=src.EVO_RECALCCOST, 
                    target.EVO_SERVICESLABOR=src.EVO_SERVICESLABOR, 
                    target.EVO_STOCKMATERIAL=src.EVO_STOCKMATERIAL, 
                    target.EVO_TOOL=src.EVO_TOOL, 
                    target.EVO_TOTAL=src.EVO_TOTAL, 
                    target.EVO_UPDATED=src.EVO_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    EVO_COSTCALCULATED, 
                    EVO_DIRECTMATERIAL, 
                    EVO_EVENT, 
                    EVO_HIREDLABOR, 
                    EVO_HOURS, 
                    EVO_LABOR, 
                    EVO_LASTSAVED, 
                    EVO_MATERIAL, 
                    EVO_OWNLABOR, 
                    EVO_RECALCCOST, 
                    EVO_SERVICESLABOR, 
                    EVO_STOCKMATERIAL, 
                    EVO_TOOL, 
                    EVO_TOTAL, 
                    EVO_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.EVO_COSTCALCULATED, 
                    src.EVO_DIRECTMATERIAL, 
                    src.EVO_EVENT, 
                    src.EVO_HIREDLABOR, 
                    src.EVO_HOURS, 
                    src.EVO_LABOR, 
                    src.EVO_LASTSAVED, 
                    src.EVO_MATERIAL, 
                    src.EVO_OWNLABOR, 
                    src.EVO_RECALCCOST, 
                    src.EVO_SERVICESLABOR, 
                    src.EVO_STOCKMATERIAL, 
                    src.EVO_TOOL, 
                    src.EVO_TOTAL, 
                    src.EVO_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5EVENTCOST_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.EVO_COSTCALCULATED, 
            strm.EVO_DIRECTMATERIAL, 
            strm.EVO_EVENT, 
            strm.EVO_HIREDLABOR, 
            strm.EVO_HOURS, 
            strm.EVO_LABOR, 
            strm.EVO_LASTSAVED, 
            strm.EVO_MATERIAL, 
            strm.EVO_OWNLABOR, 
            strm.EVO_RECALCCOST, 
            strm.EVO_SERVICESLABOR, 
            strm.EVO_STOCKMATERIAL, 
            strm.EVO_TOOL, 
            strm.EVO_TOTAL, 
            strm.EVO_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EVO_EVENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTCOST as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EVO_EVENT=src.EVO_EVENT) OR (target.EVO_EVENT IS NULL AND src.EVO_EVENT IS NULL)) 
                when matched then update set
                    target.EVO_COSTCALCULATED=src.EVO_COSTCALCULATED, 
                    target.EVO_DIRECTMATERIAL=src.EVO_DIRECTMATERIAL, 
                    target.EVO_EVENT=src.EVO_EVENT, 
                    target.EVO_HIREDLABOR=src.EVO_HIREDLABOR, 
                    target.EVO_HOURS=src.EVO_HOURS, 
                    target.EVO_LABOR=src.EVO_LABOR, 
                    target.EVO_LASTSAVED=src.EVO_LASTSAVED, 
                    target.EVO_MATERIAL=src.EVO_MATERIAL, 
                    target.EVO_OWNLABOR=src.EVO_OWNLABOR, 
                    target.EVO_RECALCCOST=src.EVO_RECALCCOST, 
                    target.EVO_SERVICESLABOR=src.EVO_SERVICESLABOR, 
                    target.EVO_STOCKMATERIAL=src.EVO_STOCKMATERIAL, 
                    target.EVO_TOOL=src.EVO_TOOL, 
                    target.EVO_TOTAL=src.EVO_TOTAL, 
                    target.EVO_UPDATED=src.EVO_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( EVO_COSTCALCULATED, EVO_DIRECTMATERIAL, EVO_EVENT, EVO_HIREDLABOR, EVO_HOURS, EVO_LABOR, EVO_LASTSAVED, EVO_MATERIAL, EVO_OWNLABOR, EVO_RECALCCOST, EVO_SERVICESLABOR, EVO_STOCKMATERIAL, EVO_TOOL, EVO_TOTAL, EVO_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.EVO_COSTCALCULATED, 
                    src.EVO_DIRECTMATERIAL, 
                    src.EVO_EVENT, 
                    src.EVO_HIREDLABOR, 
                    src.EVO_HOURS, 
                    src.EVO_LABOR, 
                    src.EVO_LASTSAVED, 
                    src.EVO_MATERIAL, 
                    src.EVO_OWNLABOR, 
                    src.EVO_RECALCCOST, 
                    src.EVO_SERVICESLABOR, 
                    src.EVO_STOCKMATERIAL, 
                    src.EVO_TOOL, 
                    src.EVO_TOTAL, 
                    src.EVO_UPDATED, 
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