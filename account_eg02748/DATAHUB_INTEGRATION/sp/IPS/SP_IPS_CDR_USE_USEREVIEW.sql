create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_USE_USEREVIEW()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEREVIEW_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_USE_USEREVIEW as target using (SELECT * FROM (SELECT 
            strm.ACTLTM, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APUSEKEY, 
            strm.APUSEREVIEWKEY, 
            strm.APUSEREVIEWTYPEKEY, 
            strm.ASSIGNED, 
            strm.ASSIGNTO, 
            strm.ASSIGNTOPROVIDER, 
            strm.CMPTRGEN, 
            strm.COMP, 
            strm.COMPBY, 
            strm.COMPBYPROVIDER, 
            strm.COMPDTTM, 
            strm.DELETED, 
            strm.DEPT, 
            strm.ISSBY, 
            strm.ISSDTTM, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.RELEVANTREVIEW, 
            strm.RESULT, 
            strm.RESULTBY, 
            strm.RESULTBYPROVIDER, 
            strm.RESULTDTTM, 
            strm.RESULTGEN, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.STARTBY, 
            strm.STARTBYPROVIDER, 
            strm.STARTDTTM, 
            strm.STARTED, 
            strm.SUSPDT, 
            strm.TYPENO, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEREVIEWKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEREVIEW as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEREVIEWKEY=src.APUSEREVIEWKEY) OR (target.APUSEREVIEWKEY IS NULL AND src.APUSEREVIEWKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTLTM=src.ACTLTM, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEKEY=src.APUSEKEY, 
                    target.APUSEREVIEWKEY=src.APUSEREVIEWKEY, 
                    target.APUSEREVIEWTYPEKEY=src.APUSEREVIEWTYPEKEY, 
                    target.ASSIGNED=src.ASSIGNED, 
                    target.ASSIGNTO=src.ASSIGNTO, 
                    target.ASSIGNTOPROVIDER=src.ASSIGNTOPROVIDER, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.COMP=src.COMP, 
                    target.COMPBY=src.COMPBY, 
                    target.COMPBYPROVIDER=src.COMPBYPROVIDER, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.DELETED=src.DELETED, 
                    target.DEPT=src.DEPT, 
                    target.ISSBY=src.ISSBY, 
                    target.ISSDTTM=src.ISSDTTM, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.RELEVANTREVIEW=src.RELEVANTREVIEW, 
                    target.RESULT=src.RESULT, 
                    target.RESULTBY=src.RESULTBY, 
                    target.RESULTBYPROVIDER=src.RESULTBYPROVIDER, 
                    target.RESULTDTTM=src.RESULTDTTM, 
                    target.RESULTGEN=src.RESULTGEN, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.STARTBY=src.STARTBY, 
                    target.STARTBYPROVIDER=src.STARTBYPROVIDER, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STARTED=src.STARTED, 
                    target.SUSPDT=src.SUSPDT, 
                    target.TYPENO=src.TYPENO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTLTM, 
                    ADDBY, 
                    ADDDTTM, 
                    APUSEKEY, 
                    APUSEREVIEWKEY, 
                    APUSEREVIEWTYPEKEY, 
                    ASSIGNED, 
                    ASSIGNTO, 
                    ASSIGNTOPROVIDER, 
                    CMPTRGEN, 
                    COMP, 
                    COMPBY, 
                    COMPBYPROVIDER, 
                    COMPDTTM, 
                    DELETED, 
                    DEPT, 
                    ISSBY, 
                    ISSDTTM, 
                    MODBY, 
                    MODDTTM, 
                    RELEVANTREVIEW, 
                    RESULT, 
                    RESULTBY, 
                    RESULTBYPROVIDER, 
                    RESULTDTTM, 
                    RESULTGEN, 
                    SCHEDBY, 
                    SCHEDDTTM, 
                    STARTBY, 
                    STARTBYPROVIDER, 
                    STARTDTTM, 
                    STARTED, 
                    SUSPDT, 
                    TYPENO, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTLTM, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APUSEKEY, 
                    src.APUSEREVIEWKEY, 
                    src.APUSEREVIEWTYPEKEY, 
                    src.ASSIGNED, 
                    src.ASSIGNTO, 
                    src.ASSIGNTOPROVIDER, 
                    src.CMPTRGEN, 
                    src.COMP, 
                    src.COMPBY, 
                    src.COMPBYPROVIDER, 
                    src.COMPDTTM, 
                    src.DELETED, 
                    src.DEPT, 
                    src.ISSBY, 
                    src.ISSDTTM, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.RELEVANTREVIEW, 
                    src.RESULT, 
                    src.RESULTBY, 
                    src.RESULTBYPROVIDER, 
                    src.RESULTDTTM, 
                    src.RESULTGEN, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.STARTBY, 
                    src.STARTBYPROVIDER, 
                    src.STARTDTTM, 
                    src.STARTED, 
                    src.SUSPDT, 
                    src.TYPENO, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_USE_USEREVIEW as target using (
                SELECT * FROM (SELECT 
            strm.ACTLTM, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APUSEKEY, 
            strm.APUSEREVIEWKEY, 
            strm.APUSEREVIEWTYPEKEY, 
            strm.ASSIGNED, 
            strm.ASSIGNTO, 
            strm.ASSIGNTOPROVIDER, 
            strm.CMPTRGEN, 
            strm.COMP, 
            strm.COMPBY, 
            strm.COMPBYPROVIDER, 
            strm.COMPDTTM, 
            strm.DELETED, 
            strm.DEPT, 
            strm.ISSBY, 
            strm.ISSDTTM, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.RELEVANTREVIEW, 
            strm.RESULT, 
            strm.RESULTBY, 
            strm.RESULTBYPROVIDER, 
            strm.RESULTDTTM, 
            strm.RESULTGEN, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.STARTBY, 
            strm.STARTBYPROVIDER, 
            strm.STARTDTTM, 
            strm.STARTED, 
            strm.SUSPDT, 
            strm.TYPENO, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEREVIEWKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEREVIEW as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEREVIEWKEY=src.APUSEREVIEWKEY) OR (target.APUSEREVIEWKEY IS NULL AND src.APUSEREVIEWKEY IS NULL)) 
                when matched then update set
                    target.ACTLTM=src.ACTLTM, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEKEY=src.APUSEKEY, 
                    target.APUSEREVIEWKEY=src.APUSEREVIEWKEY, 
                    target.APUSEREVIEWTYPEKEY=src.APUSEREVIEWTYPEKEY, 
                    target.ASSIGNED=src.ASSIGNED, 
                    target.ASSIGNTO=src.ASSIGNTO, 
                    target.ASSIGNTOPROVIDER=src.ASSIGNTOPROVIDER, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.COMP=src.COMP, 
                    target.COMPBY=src.COMPBY, 
                    target.COMPBYPROVIDER=src.COMPBYPROVIDER, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.DELETED=src.DELETED, 
                    target.DEPT=src.DEPT, 
                    target.ISSBY=src.ISSBY, 
                    target.ISSDTTM=src.ISSDTTM, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.RELEVANTREVIEW=src.RELEVANTREVIEW, 
                    target.RESULT=src.RESULT, 
                    target.RESULTBY=src.RESULTBY, 
                    target.RESULTBYPROVIDER=src.RESULTBYPROVIDER, 
                    target.RESULTDTTM=src.RESULTDTTM, 
                    target.RESULTGEN=src.RESULTGEN, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.STARTBY=src.STARTBY, 
                    target.STARTBYPROVIDER=src.STARTBYPROVIDER, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STARTED=src.STARTED, 
                    target.SUSPDT=src.SUSPDT, 
                    target.TYPENO=src.TYPENO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTLTM, ADDBY, ADDDTTM, APUSEKEY, APUSEREVIEWKEY, APUSEREVIEWTYPEKEY, ASSIGNED, ASSIGNTO, ASSIGNTOPROVIDER, CMPTRGEN, COMP, COMPBY, COMPBYPROVIDER, COMPDTTM, DELETED, DEPT, ISSBY, ISSDTTM, MODBY, MODDTTM, RELEVANTREVIEW, RESULT, RESULTBY, RESULTBYPROVIDER, RESULTDTTM, RESULTGEN, SCHEDBY, SCHEDDTTM, STARTBY, STARTBYPROVIDER, STARTDTTM, STARTED, SUSPDT, TYPENO, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTLTM, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APUSEKEY, 
                    src.APUSEREVIEWKEY, 
                    src.APUSEREVIEWTYPEKEY, 
                    src.ASSIGNED, 
                    src.ASSIGNTO, 
                    src.ASSIGNTOPROVIDER, 
                    src.CMPTRGEN, 
                    src.COMP, 
                    src.COMPBY, 
                    src.COMPBYPROVIDER, 
                    src.COMPDTTM, 
                    src.DELETED, 
                    src.DEPT, 
                    src.ISSBY, 
                    src.ISSDTTM, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.RELEVANTREVIEW, 
                    src.RESULT, 
                    src.RESULTBY, 
                    src.RESULTBYPROVIDER, 
                    src.RESULTDTTM, 
                    src.RESULTGEN, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.STARTBY, 
                    src.STARTBYPROVIDER, 
                    src.STARTDTTM, 
                    src.STARTED, 
                    src.SUSPDT, 
                    src.TYPENO, 
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