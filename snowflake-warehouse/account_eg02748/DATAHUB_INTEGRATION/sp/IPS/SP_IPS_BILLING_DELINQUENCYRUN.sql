create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_DELINQUENCYRUN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYRUN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_DELINQUENCYRUN as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTAREA, 
            strm.ACCOUNTBILLINGCYCLE, 
            strm.ACCOUNTCLASS, 
            strm.ACCOUNTGROUP, 
            strm.ACCOUNTSUBGROUP, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTODECANDRESOLVEFLAG, 
            strm.BILLTYPEKEY, 
            strm.COMMITNUMBER, 
            strm.DELETED, 
            strm.DELINQUENCYPROCESSTYPE, 
            strm.DELINQUENCYRUNKEY, 
            strm.ENDADVANCEMENTRUN, 
            strm.ENDENTRYRUN, 
            strm.INITIATEDBY, 
            strm.INITIATEDTTM, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFADVANCEDACCOUNTS, 
            strm.NUMBEROFENTRYACCOUNTS, 
            strm.NUMBEROFEXCEPTIONS, 
            strm.PROCESSINGFLAG, 
            strm.SCHEDULEKEY, 
            strm.STARTADVANCEMENTRUN, 
            strm.STARTENTRYRUN, 
            strm.TOTALNUMBEROFACCOUNTS, 
            strm.VARIATION_ID, 
            strm.WATERMETERADDRESSROUTEKEY, 
            strm.WATERMETERREADINGCYCLE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DELINQUENCYRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYRUN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY) OR (target.DELINQUENCYRUNKEY IS NULL AND src.DELINQUENCYRUNKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTAREA=src.ACCOUNTAREA, 
                    target.ACCOUNTBILLINGCYCLE=src.ACCOUNTBILLINGCYCLE, 
                    target.ACCOUNTCLASS=src.ACCOUNTCLASS, 
                    target.ACCOUNTGROUP=src.ACCOUNTGROUP, 
                    target.ACCOUNTSUBGROUP=src.ACCOUNTSUBGROUP, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTODECANDRESOLVEFLAG=src.AUTODECANDRESOLVEFLAG, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMITNUMBER=src.COMMITNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYPROCESSTYPE=src.DELINQUENCYPROCESSTYPE, 
                    target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY, 
                    target.ENDADVANCEMENTRUN=src.ENDADVANCEMENTRUN, 
                    target.ENDENTRYRUN=src.ENDENTRYRUN, 
                    target.INITIATEDBY=src.INITIATEDBY, 
                    target.INITIATEDTTM=src.INITIATEDTTM, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFADVANCEDACCOUNTS=src.NUMBEROFADVANCEDACCOUNTS, 
                    target.NUMBEROFENTRYACCOUNTS=src.NUMBEROFENTRYACCOUNTS, 
                    target.NUMBEROFEXCEPTIONS=src.NUMBEROFEXCEPTIONS, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.STARTADVANCEMENTRUN=src.STARTADVANCEMENTRUN, 
                    target.STARTENTRYRUN=src.STARTENTRYRUN, 
                    target.TOTALNUMBEROFACCOUNTS=src.TOTALNUMBEROFACCOUNTS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMETERADDRESSROUTEKEY=src.WATERMETERADDRESSROUTEKEY, 
                    target.WATERMETERREADINGCYCLE=src.WATERMETERREADINGCYCLE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTAREA, 
                    ACCOUNTBILLINGCYCLE, 
                    ACCOUNTCLASS, 
                    ACCOUNTGROUP, 
                    ACCOUNTSUBGROUP, 
                    ADDBY, 
                    ADDDTTM, 
                    AUTODECANDRESOLVEFLAG, 
                    BILLTYPEKEY, 
                    COMMITNUMBER, 
                    DELETED, 
                    DELINQUENCYPROCESSTYPE, 
                    DELINQUENCYRUNKEY, 
                    ENDADVANCEMENTRUN, 
                    ENDENTRYRUN, 
                    INITIATEDBY, 
                    INITIATEDTTM, 
                    LASTINVOCATIONSTATUS, 
                    MODBY, 
                    MODDTTM, 
                    NUMBEROFADVANCEDACCOUNTS, 
                    NUMBEROFENTRYACCOUNTS, 
                    NUMBEROFEXCEPTIONS, 
                    PROCESSINGFLAG, 
                    SCHEDULEKEY, 
                    STARTADVANCEMENTRUN, 
                    STARTENTRYRUN, 
                    TOTALNUMBEROFACCOUNTS, 
                    VARIATION_ID, 
                    WATERMETERADDRESSROUTEKEY, 
                    WATERMETERREADINGCYCLE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTAREA, 
                    src.ACCOUNTBILLINGCYCLE, 
                    src.ACCOUNTCLASS, 
                    src.ACCOUNTGROUP, 
                    src.ACCOUNTSUBGROUP, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTODECANDRESOLVEFLAG, 
                    src.BILLTYPEKEY, 
                    src.COMMITNUMBER, 
                    src.DELETED, 
                    src.DELINQUENCYPROCESSTYPE, 
                    src.DELINQUENCYRUNKEY, 
                    src.ENDADVANCEMENTRUN, 
                    src.ENDENTRYRUN, 
                    src.INITIATEDBY, 
                    src.INITIATEDTTM, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFADVANCEDACCOUNTS, 
                    src.NUMBEROFENTRYACCOUNTS, 
                    src.NUMBEROFEXCEPTIONS, 
                    src.PROCESSINGFLAG, 
                    src.SCHEDULEKEY, 
                    src.STARTADVANCEMENTRUN, 
                    src.STARTENTRYRUN, 
                    src.TOTALNUMBEROFACCOUNTS, 
                    src.VARIATION_ID, 
                    src.WATERMETERADDRESSROUTEKEY, 
                    src.WATERMETERREADINGCYCLE,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYRUN as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTAREA, 
            strm.ACCOUNTBILLINGCYCLE, 
            strm.ACCOUNTCLASS, 
            strm.ACCOUNTGROUP, 
            strm.ACCOUNTSUBGROUP, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTODECANDRESOLVEFLAG, 
            strm.BILLTYPEKEY, 
            strm.COMMITNUMBER, 
            strm.DELETED, 
            strm.DELINQUENCYPROCESSTYPE, 
            strm.DELINQUENCYRUNKEY, 
            strm.ENDADVANCEMENTRUN, 
            strm.ENDENTRYRUN, 
            strm.INITIATEDBY, 
            strm.INITIATEDTTM, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFADVANCEDACCOUNTS, 
            strm.NUMBEROFENTRYACCOUNTS, 
            strm.NUMBEROFEXCEPTIONS, 
            strm.PROCESSINGFLAG, 
            strm.SCHEDULEKEY, 
            strm.STARTADVANCEMENTRUN, 
            strm.STARTENTRYRUN, 
            strm.TOTALNUMBEROFACCOUNTS, 
            strm.VARIATION_ID, 
            strm.WATERMETERADDRESSROUTEKEY, 
            strm.WATERMETERREADINGCYCLE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DELINQUENCYRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYRUN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY) OR (target.DELINQUENCYRUNKEY IS NULL AND src.DELINQUENCYRUNKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTAREA=src.ACCOUNTAREA, 
                    target.ACCOUNTBILLINGCYCLE=src.ACCOUNTBILLINGCYCLE, 
                    target.ACCOUNTCLASS=src.ACCOUNTCLASS, 
                    target.ACCOUNTGROUP=src.ACCOUNTGROUP, 
                    target.ACCOUNTSUBGROUP=src.ACCOUNTSUBGROUP, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTODECANDRESOLVEFLAG=src.AUTODECANDRESOLVEFLAG, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMITNUMBER=src.COMMITNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYPROCESSTYPE=src.DELINQUENCYPROCESSTYPE, 
                    target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY, 
                    target.ENDADVANCEMENTRUN=src.ENDADVANCEMENTRUN, 
                    target.ENDENTRYRUN=src.ENDENTRYRUN, 
                    target.INITIATEDBY=src.INITIATEDBY, 
                    target.INITIATEDTTM=src.INITIATEDTTM, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFADVANCEDACCOUNTS=src.NUMBEROFADVANCEDACCOUNTS, 
                    target.NUMBEROFENTRYACCOUNTS=src.NUMBEROFENTRYACCOUNTS, 
                    target.NUMBEROFEXCEPTIONS=src.NUMBEROFEXCEPTIONS, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.STARTADVANCEMENTRUN=src.STARTADVANCEMENTRUN, 
                    target.STARTENTRYRUN=src.STARTENTRYRUN, 
                    target.TOTALNUMBEROFACCOUNTS=src.TOTALNUMBEROFACCOUNTS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMETERADDRESSROUTEKEY=src.WATERMETERADDRESSROUTEKEY, 
                    target.WATERMETERREADINGCYCLE=src.WATERMETERREADINGCYCLE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTAREA, ACCOUNTBILLINGCYCLE, ACCOUNTCLASS, ACCOUNTGROUP, ACCOUNTSUBGROUP, ADDBY, ADDDTTM, AUTODECANDRESOLVEFLAG, BILLTYPEKEY, COMMITNUMBER, DELETED, DELINQUENCYPROCESSTYPE, DELINQUENCYRUNKEY, ENDADVANCEMENTRUN, ENDENTRYRUN, INITIATEDBY, INITIATEDTTM, LASTINVOCATIONSTATUS, MODBY, MODDTTM, NUMBEROFADVANCEDACCOUNTS, NUMBEROFENTRYACCOUNTS, NUMBEROFEXCEPTIONS, PROCESSINGFLAG, SCHEDULEKEY, STARTADVANCEMENTRUN, STARTENTRYRUN, TOTALNUMBEROFACCOUNTS, VARIATION_ID, WATERMETERADDRESSROUTEKEY, WATERMETERREADINGCYCLE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTAREA, 
                    src.ACCOUNTBILLINGCYCLE, 
                    src.ACCOUNTCLASS, 
                    src.ACCOUNTGROUP, 
                    src.ACCOUNTSUBGROUP, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTODECANDRESOLVEFLAG, 
                    src.BILLTYPEKEY, 
                    src.COMMITNUMBER, 
                    src.DELETED, 
                    src.DELINQUENCYPROCESSTYPE, 
                    src.DELINQUENCYRUNKEY, 
                    src.ENDADVANCEMENTRUN, 
                    src.ENDENTRYRUN, 
                    src.INITIATEDBY, 
                    src.INITIATEDTTM, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFADVANCEDACCOUNTS, 
                    src.NUMBEROFENTRYACCOUNTS, 
                    src.NUMBEROFEXCEPTIONS, 
                    src.PROCESSINGFLAG, 
                    src.SCHEDULEKEY, 
                    src.STARTADVANCEMENTRUN, 
                    src.STARTENTRYRUN, 
                    src.TOTALNUMBEROFACCOUNTS, 
                    src.VARIATION_ID, 
                    src.WATERMETERADDRESSROUTEKEY, 
                    src.WATERMETERREADINGCYCLE,     
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