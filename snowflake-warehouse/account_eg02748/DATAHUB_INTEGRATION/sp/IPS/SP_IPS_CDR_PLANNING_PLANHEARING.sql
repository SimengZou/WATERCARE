create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_PLANNING_PLANHEARING()
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
                            INSERT INTO LANDING_ERROR.IPS_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANHEARING_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_PLANNING_PLANHEARING as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICATIONKEY, 
            strm.COMMENTS, 
            strm.COMPDTTM, 
            strm.COMPFLAG, 
            strm.DELETED, 
            strm.DOCKETNUM, 
            strm.HEARTYPEKEY, 
            strm.LOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NAME, 
            strm.PLANHEARKEY, 
            strm.RECORDBY, 
            strm.RESULT, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.SCHEDFLAG, 
            strm.STARTDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PLANHEARKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANHEARING as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PLANHEARKEY=src.PLANHEARKEY) OR (target.PLANHEARKEY IS NULL AND src.PLANHEARKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICATIONKEY=src.APPLICATIONKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.COMPFLAG=src.COMPFLAG, 
                    target.DELETED=src.DELETED, 
                    target.DOCKETNUM=src.DOCKETNUM, 
                    target.HEARTYPEKEY=src.HEARTYPEKEY, 
                    target.LOCATION=src.LOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NAME=src.NAME, 
                    target.PLANHEARKEY=src.PLANHEARKEY, 
                    target.RECORDBY=src.RECORDBY, 
                    target.RESULT=src.RESULT, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.SCHEDFLAG=src.SCHEDFLAG, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPLICATIONKEY, 
                    COMMENTS, 
                    COMPDTTM, 
                    COMPFLAG, 
                    DELETED, 
                    DOCKETNUM, 
                    HEARTYPEKEY, 
                    LOCATION, 
                    MODBY, 
                    MODDTTM, 
                    NAME, 
                    PLANHEARKEY, 
                    RECORDBY, 
                    RESULT, 
                    SCHEDBY, 
                    SCHEDDTTM, 
                    SCHEDFLAG, 
                    STARTDTTM, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICATIONKEY, 
                    src.COMMENTS, 
                    src.COMPDTTM, 
                    src.COMPFLAG, 
                    src.DELETED, 
                    src.DOCKETNUM, 
                    src.HEARTYPEKEY, 
                    src.LOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NAME, 
                    src.PLANHEARKEY, 
                    src.RECORDBY, 
                    src.RESULT, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.SCHEDFLAG, 
                    src.STARTDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PLANNING_PLANHEARING as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICATIONKEY, 
            strm.COMMENTS, 
            strm.COMPDTTM, 
            strm.COMPFLAG, 
            strm.DELETED, 
            strm.DOCKETNUM, 
            strm.HEARTYPEKEY, 
            strm.LOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NAME, 
            strm.PLANHEARKEY, 
            strm.RECORDBY, 
            strm.RESULT, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.SCHEDFLAG, 
            strm.STARTDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PLANHEARKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANHEARING as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PLANHEARKEY=src.PLANHEARKEY) OR (target.PLANHEARKEY IS NULL AND src.PLANHEARKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICATIONKEY=src.APPLICATIONKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.COMPFLAG=src.COMPFLAG, 
                    target.DELETED=src.DELETED, 
                    target.DOCKETNUM=src.DOCKETNUM, 
                    target.HEARTYPEKEY=src.HEARTYPEKEY, 
                    target.LOCATION=src.LOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NAME=src.NAME, 
                    target.PLANHEARKEY=src.PLANHEARKEY, 
                    target.RECORDBY=src.RECORDBY, 
                    target.RESULT=src.RESULT, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.SCHEDFLAG=src.SCHEDFLAG, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPLICATIONKEY, COMMENTS, COMPDTTM, COMPFLAG, DELETED, DOCKETNUM, HEARTYPEKEY, LOCATION, MODBY, MODDTTM, NAME, PLANHEARKEY, RECORDBY, RESULT, SCHEDBY, SCHEDDTTM, SCHEDFLAG, STARTDTTM, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICATIONKEY, 
                    src.COMMENTS, 
                    src.COMPDTTM, 
                    src.COMPFLAG, 
                    src.DELETED, 
                    src.DOCKETNUM, 
                    src.HEARTYPEKEY, 
                    src.LOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NAME, 
                    src.PLANHEARKEY, 
                    src.RECORDBY, 
                    src.RESULT, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.SCHEDFLAG, 
                    src.STARTDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
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