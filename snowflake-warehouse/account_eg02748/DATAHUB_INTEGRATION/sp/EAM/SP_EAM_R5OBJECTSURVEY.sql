create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5OBJECTSURVEY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5OBJECTSURVEY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5OBJECTSURVEY as target using (SELECT * FROM (SELECT 
            strm.OBS_ANSWERPK, 
            strm.OBS_CALCULATEDANSWER, 
            strm.OBS_CALCULATEDVALUE, 
            strm.OBS_DATEEFFECTIVE, 
            strm.OBS_DATELASTCALCULATED, 
            strm.OBS_LASTSAVED, 
            strm.OBS_LEVELPK, 
            strm.OBS_OBJECT, 
            strm.OBS_OBJECT_ORG, 
            strm.OBS_OPERATORCHECKLIST, 
            strm.OBS_TYPE, 
            strm.OBS_UPDATECOUNT, 
            strm.OBS_VALUE, 
            strm.OBS_WORKORDER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.OBS_OBJECT,
            strm.OBS_OBJECT_ORG,
            strm.OBS_TYPE,
            strm.OBS_LEVELPK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5OBJECTSURVEY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.OBS_OBJECT=src.OBS_OBJECT) OR (target.OBS_OBJECT IS NULL AND src.OBS_OBJECT IS NULL)) AND
            ((target.OBS_OBJECT_ORG=src.OBS_OBJECT_ORG) OR (target.OBS_OBJECT_ORG IS NULL AND src.OBS_OBJECT_ORG IS NULL)) AND
            ((target.OBS_TYPE=src.OBS_TYPE) OR (target.OBS_TYPE IS NULL AND src.OBS_TYPE IS NULL)) AND
            ((target.OBS_LEVELPK=src.OBS_LEVELPK) OR (target.OBS_LEVELPK IS NULL AND src.OBS_LEVELPK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.OBS_ANSWERPK=src.OBS_ANSWERPK, 
                    target.OBS_CALCULATEDANSWER=src.OBS_CALCULATEDANSWER, 
                    target.OBS_CALCULATEDVALUE=src.OBS_CALCULATEDVALUE, 
                    target.OBS_DATEEFFECTIVE=src.OBS_DATEEFFECTIVE, 
                    target.OBS_DATELASTCALCULATED=src.OBS_DATELASTCALCULATED, 
                    target.OBS_LASTSAVED=src.OBS_LASTSAVED, 
                    target.OBS_LEVELPK=src.OBS_LEVELPK, 
                    target.OBS_OBJECT=src.OBS_OBJECT, 
                    target.OBS_OBJECT_ORG=src.OBS_OBJECT_ORG, 
                    target.OBS_OPERATORCHECKLIST=src.OBS_OPERATORCHECKLIST, 
                    target.OBS_TYPE=src.OBS_TYPE, 
                    target.OBS_UPDATECOUNT=src.OBS_UPDATECOUNT, 
                    target.OBS_VALUE=src.OBS_VALUE, 
                    target.OBS_WORKORDER=src.OBS_WORKORDER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    OBS_ANSWERPK, 
                    OBS_CALCULATEDANSWER, 
                    OBS_CALCULATEDVALUE, 
                    OBS_DATEEFFECTIVE, 
                    OBS_DATELASTCALCULATED, 
                    OBS_LASTSAVED, 
                    OBS_LEVELPK, 
                    OBS_OBJECT, 
                    OBS_OBJECT_ORG, 
                    OBS_OPERATORCHECKLIST, 
                    OBS_TYPE, 
                    OBS_UPDATECOUNT, 
                    OBS_VALUE, 
                    OBS_WORKORDER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.OBS_ANSWERPK, 
                    src.OBS_CALCULATEDANSWER, 
                    src.OBS_CALCULATEDVALUE, 
                    src.OBS_DATEEFFECTIVE, 
                    src.OBS_DATELASTCALCULATED, 
                    src.OBS_LASTSAVED, 
                    src.OBS_LEVELPK, 
                    src.OBS_OBJECT, 
                    src.OBS_OBJECT_ORG, 
                    src.OBS_OPERATORCHECKLIST, 
                    src.OBS_TYPE, 
                    src.OBS_UPDATECOUNT, 
                    src.OBS_VALUE, 
                    src.OBS_WORKORDER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5OBJECTSURVEY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.OBS_ANSWERPK, 
            strm.OBS_CALCULATEDANSWER, 
            strm.OBS_CALCULATEDVALUE, 
            strm.OBS_DATEEFFECTIVE, 
            strm.OBS_DATELASTCALCULATED, 
            strm.OBS_LASTSAVED, 
            strm.OBS_LEVELPK, 
            strm.OBS_OBJECT, 
            strm.OBS_OBJECT_ORG, 
            strm.OBS_OPERATORCHECKLIST, 
            strm.OBS_TYPE, 
            strm.OBS_UPDATECOUNT, 
            strm.OBS_VALUE, 
            strm.OBS_WORKORDER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.OBS_OBJECT,
            strm.OBS_OBJECT_ORG,
            strm.OBS_TYPE,
            strm.OBS_LEVELPK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5OBJECTSURVEY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.OBS_OBJECT=src.OBS_OBJECT) OR (target.OBS_OBJECT IS NULL AND src.OBS_OBJECT IS NULL)) AND
            ((target.OBS_OBJECT_ORG=src.OBS_OBJECT_ORG) OR (target.OBS_OBJECT_ORG IS NULL AND src.OBS_OBJECT_ORG IS NULL)) AND
            ((target.OBS_TYPE=src.OBS_TYPE) OR (target.OBS_TYPE IS NULL AND src.OBS_TYPE IS NULL)) AND
            ((target.OBS_LEVELPK=src.OBS_LEVELPK) OR (target.OBS_LEVELPK IS NULL AND src.OBS_LEVELPK IS NULL)) 
                when matched then update set
                    target.OBS_ANSWERPK=src.OBS_ANSWERPK, 
                    target.OBS_CALCULATEDANSWER=src.OBS_CALCULATEDANSWER, 
                    target.OBS_CALCULATEDVALUE=src.OBS_CALCULATEDVALUE, 
                    target.OBS_DATEEFFECTIVE=src.OBS_DATEEFFECTIVE, 
                    target.OBS_DATELASTCALCULATED=src.OBS_DATELASTCALCULATED, 
                    target.OBS_LASTSAVED=src.OBS_LASTSAVED, 
                    target.OBS_LEVELPK=src.OBS_LEVELPK, 
                    target.OBS_OBJECT=src.OBS_OBJECT, 
                    target.OBS_OBJECT_ORG=src.OBS_OBJECT_ORG, 
                    target.OBS_OPERATORCHECKLIST=src.OBS_OPERATORCHECKLIST, 
                    target.OBS_TYPE=src.OBS_TYPE, 
                    target.OBS_UPDATECOUNT=src.OBS_UPDATECOUNT, 
                    target.OBS_VALUE=src.OBS_VALUE, 
                    target.OBS_WORKORDER=src.OBS_WORKORDER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( OBS_ANSWERPK, OBS_CALCULATEDANSWER, OBS_CALCULATEDVALUE, OBS_DATEEFFECTIVE, OBS_DATELASTCALCULATED, OBS_LASTSAVED, OBS_LEVELPK, OBS_OBJECT, OBS_OBJECT_ORG, OBS_OPERATORCHECKLIST, OBS_TYPE, OBS_UPDATECOUNT, OBS_VALUE, OBS_WORKORDER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.OBS_ANSWERPK, 
                    src.OBS_CALCULATEDANSWER, 
                    src.OBS_CALCULATEDVALUE, 
                    src.OBS_DATEEFFECTIVE, 
                    src.OBS_DATELASTCALCULATED, 
                    src.OBS_LASTSAVED, 
                    src.OBS_LEVELPK, 
                    src.OBS_OBJECT, 
                    src.OBS_OBJECT_ORG, 
                    src.OBS_OPERATORCHECKLIST, 
                    src.OBS_TYPE, 
                    src.OBS_UPDATECOUNT, 
                    src.OBS_VALUE, 
                    src.OBS_WORKORDER, 
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