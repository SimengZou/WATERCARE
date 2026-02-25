create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5WORKPACKAGES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5WORKPACKAGES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5WORKPACKAGES as target using (SELECT * FROM (SELECT 
            strm.WPK_CHANGED, 
            strm.WPK_CLASS, 
            strm.WPK_CLASS_ORG, 
            strm.WPK_CODE, 
            strm.WPK_DESC, 
            strm.WPK_DUEDATE, 
            strm.WPK_DURATION, 
            strm.WPK_ESTWORKLOAD, 
            strm.WPK_FREQ, 
            strm.WPK_JOBCLASS, 
            strm.WPK_JOBCLASS_ORG, 
            strm.WPK_JOBTYPE, 
            strm.WPK_LASTEVENT, 
            strm.WPK_LASTSAVED, 
            strm.WPK_MRC, 
            strm.WPK_OBJECT, 
            strm.WPK_OBJECT_ORG, 
            strm.WPK_ORG, 
            strm.WPK_PERFORMONDAY, 
            strm.WPK_PERFORMONWEEK, 
            strm.WPK_PERIODUOM, 
            strm.WPK_PERSONS, 
            strm.WPK_PPMTYPE, 
            strm.WPK_STATUS, 
            strm.WPK_TRADE, 
            strm.WPK_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WPK_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WORKPACKAGES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WPK_CODE=src.WPK_CODE) OR (target.WPK_CODE IS NULL AND src.WPK_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.WPK_CHANGED=src.WPK_CHANGED, 
                    target.WPK_CLASS=src.WPK_CLASS, 
                    target.WPK_CLASS_ORG=src.WPK_CLASS_ORG, 
                    target.WPK_CODE=src.WPK_CODE, 
                    target.WPK_DESC=src.WPK_DESC, 
                    target.WPK_DUEDATE=src.WPK_DUEDATE, 
                    target.WPK_DURATION=src.WPK_DURATION, 
                    target.WPK_ESTWORKLOAD=src.WPK_ESTWORKLOAD, 
                    target.WPK_FREQ=src.WPK_FREQ, 
                    target.WPK_JOBCLASS=src.WPK_JOBCLASS, 
                    target.WPK_JOBCLASS_ORG=src.WPK_JOBCLASS_ORG, 
                    target.WPK_JOBTYPE=src.WPK_JOBTYPE, 
                    target.WPK_LASTEVENT=src.WPK_LASTEVENT, 
                    target.WPK_LASTSAVED=src.WPK_LASTSAVED, 
                    target.WPK_MRC=src.WPK_MRC, 
                    target.WPK_OBJECT=src.WPK_OBJECT, 
                    target.WPK_OBJECT_ORG=src.WPK_OBJECT_ORG, 
                    target.WPK_ORG=src.WPK_ORG, 
                    target.WPK_PERFORMONDAY=src.WPK_PERFORMONDAY, 
                    target.WPK_PERFORMONWEEK=src.WPK_PERFORMONWEEK, 
                    target.WPK_PERIODUOM=src.WPK_PERIODUOM, 
                    target.WPK_PERSONS=src.WPK_PERSONS, 
                    target.WPK_PPMTYPE=src.WPK_PPMTYPE, 
                    target.WPK_STATUS=src.WPK_STATUS, 
                    target.WPK_TRADE=src.WPK_TRADE, 
                    target.WPK_UPDATECOUNT=src.WPK_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    WPK_CHANGED, 
                    WPK_CLASS, 
                    WPK_CLASS_ORG, 
                    WPK_CODE, 
                    WPK_DESC, 
                    WPK_DUEDATE, 
                    WPK_DURATION, 
                    WPK_ESTWORKLOAD, 
                    WPK_FREQ, 
                    WPK_JOBCLASS, 
                    WPK_JOBCLASS_ORG, 
                    WPK_JOBTYPE, 
                    WPK_LASTEVENT, 
                    WPK_LASTSAVED, 
                    WPK_MRC, 
                    WPK_OBJECT, 
                    WPK_OBJECT_ORG, 
                    WPK_ORG, 
                    WPK_PERFORMONDAY, 
                    WPK_PERFORMONWEEK, 
                    WPK_PERIODUOM, 
                    WPK_PERSONS, 
                    WPK_PPMTYPE, 
                    WPK_STATUS, 
                    WPK_TRADE, 
                    WPK_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.WPK_CHANGED, 
                    src.WPK_CLASS, 
                    src.WPK_CLASS_ORG, 
                    src.WPK_CODE, 
                    src.WPK_DESC, 
                    src.WPK_DUEDATE, 
                    src.WPK_DURATION, 
                    src.WPK_ESTWORKLOAD, 
                    src.WPK_FREQ, 
                    src.WPK_JOBCLASS, 
                    src.WPK_JOBCLASS_ORG, 
                    src.WPK_JOBTYPE, 
                    src.WPK_LASTEVENT, 
                    src.WPK_LASTSAVED, 
                    src.WPK_MRC, 
                    src.WPK_OBJECT, 
                    src.WPK_OBJECT_ORG, 
                    src.WPK_ORG, 
                    src.WPK_PERFORMONDAY, 
                    src.WPK_PERFORMONWEEK, 
                    src.WPK_PERIODUOM, 
                    src.WPK_PERSONS, 
                    src.WPK_PPMTYPE, 
                    src.WPK_STATUS, 
                    src.WPK_TRADE, 
                    src.WPK_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5WORKPACKAGES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.WPK_CHANGED, 
            strm.WPK_CLASS, 
            strm.WPK_CLASS_ORG, 
            strm.WPK_CODE, 
            strm.WPK_DESC, 
            strm.WPK_DUEDATE, 
            strm.WPK_DURATION, 
            strm.WPK_ESTWORKLOAD, 
            strm.WPK_FREQ, 
            strm.WPK_JOBCLASS, 
            strm.WPK_JOBCLASS_ORG, 
            strm.WPK_JOBTYPE, 
            strm.WPK_LASTEVENT, 
            strm.WPK_LASTSAVED, 
            strm.WPK_MRC, 
            strm.WPK_OBJECT, 
            strm.WPK_OBJECT_ORG, 
            strm.WPK_ORG, 
            strm.WPK_PERFORMONDAY, 
            strm.WPK_PERFORMONWEEK, 
            strm.WPK_PERIODUOM, 
            strm.WPK_PERSONS, 
            strm.WPK_PPMTYPE, 
            strm.WPK_STATUS, 
            strm.WPK_TRADE, 
            strm.WPK_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WPK_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WORKPACKAGES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WPK_CODE=src.WPK_CODE) OR (target.WPK_CODE IS NULL AND src.WPK_CODE IS NULL)) 
                when matched then update set
                    target.WPK_CHANGED=src.WPK_CHANGED, 
                    target.WPK_CLASS=src.WPK_CLASS, 
                    target.WPK_CLASS_ORG=src.WPK_CLASS_ORG, 
                    target.WPK_CODE=src.WPK_CODE, 
                    target.WPK_DESC=src.WPK_DESC, 
                    target.WPK_DUEDATE=src.WPK_DUEDATE, 
                    target.WPK_DURATION=src.WPK_DURATION, 
                    target.WPK_ESTWORKLOAD=src.WPK_ESTWORKLOAD, 
                    target.WPK_FREQ=src.WPK_FREQ, 
                    target.WPK_JOBCLASS=src.WPK_JOBCLASS, 
                    target.WPK_JOBCLASS_ORG=src.WPK_JOBCLASS_ORG, 
                    target.WPK_JOBTYPE=src.WPK_JOBTYPE, 
                    target.WPK_LASTEVENT=src.WPK_LASTEVENT, 
                    target.WPK_LASTSAVED=src.WPK_LASTSAVED, 
                    target.WPK_MRC=src.WPK_MRC, 
                    target.WPK_OBJECT=src.WPK_OBJECT, 
                    target.WPK_OBJECT_ORG=src.WPK_OBJECT_ORG, 
                    target.WPK_ORG=src.WPK_ORG, 
                    target.WPK_PERFORMONDAY=src.WPK_PERFORMONDAY, 
                    target.WPK_PERFORMONWEEK=src.WPK_PERFORMONWEEK, 
                    target.WPK_PERIODUOM=src.WPK_PERIODUOM, 
                    target.WPK_PERSONS=src.WPK_PERSONS, 
                    target.WPK_PPMTYPE=src.WPK_PPMTYPE, 
                    target.WPK_STATUS=src.WPK_STATUS, 
                    target.WPK_TRADE=src.WPK_TRADE, 
                    target.WPK_UPDATECOUNT=src.WPK_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( WPK_CHANGED, WPK_CLASS, WPK_CLASS_ORG, WPK_CODE, WPK_DESC, WPK_DUEDATE, WPK_DURATION, WPK_ESTWORKLOAD, WPK_FREQ, WPK_JOBCLASS, WPK_JOBCLASS_ORG, WPK_JOBTYPE, WPK_LASTEVENT, WPK_LASTSAVED, WPK_MRC, WPK_OBJECT, WPK_OBJECT_ORG, WPK_ORG, WPK_PERFORMONDAY, WPK_PERFORMONWEEK, WPK_PERIODUOM, WPK_PERSONS, WPK_PPMTYPE, WPK_STATUS, WPK_TRADE, WPK_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.WPK_CHANGED, 
                    src.WPK_CLASS, 
                    src.WPK_CLASS_ORG, 
                    src.WPK_CODE, 
                    src.WPK_DESC, 
                    src.WPK_DUEDATE, 
                    src.WPK_DURATION, 
                    src.WPK_ESTWORKLOAD, 
                    src.WPK_FREQ, 
                    src.WPK_JOBCLASS, 
                    src.WPK_JOBCLASS_ORG, 
                    src.WPK_JOBTYPE, 
                    src.WPK_LASTEVENT, 
                    src.WPK_LASTSAVED, 
                    src.WPK_MRC, 
                    src.WPK_OBJECT, 
                    src.WPK_OBJECT_ORG, 
                    src.WPK_ORG, 
                    src.WPK_PERFORMONDAY, 
                    src.WPK_PERFORMONWEEK, 
                    src.WPK_PERIODUOM, 
                    src.WPK_PERSONS, 
                    src.WPK_PPMTYPE, 
                    src.WPK_STATUS, 
                    src.WPK_TRADE, 
                    src.WPK_UPDATECOUNT, 
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