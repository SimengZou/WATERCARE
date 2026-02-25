create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_BILLRUN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLRUN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_BILLRUN as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTAREA, 
            strm.ACCOUNTCLASS, 
            strm.ACCOUNTGROUP, 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTSUBGROUP, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLDATE, 
            strm.BILLINGCYCLE, 
            strm.BILLINGPERIODFROMDATE, 
            strm.BILLINGPERIODTODATE, 
            strm.BILLINGSTATUS, 
            strm.BILLKEY, 
            strm.BILLRUNKEY, 
            strm.BILLRUNSCHEDULEKEY, 
            strm.BILLTYPEKEY, 
            strm.CALCULATEDFLAG, 
            strm.CALCULATINGFLAG, 
            strm.CDRAPDEFNKEY, 
            strm.CDRAPPROCESSSTATEKEY, 
            strm.CDRPRODFAMILY, 
            strm.COMMITNUMBER, 
            strm.DELETED, 
            strm.DESCRIPTION, 
            strm.DUEDATE, 
            strm.EXCLUDEFINALBILLSFLAG, 
            strm.INCLUDESUBBILLTYPESFLAG, 
            strm.INITIALIZEPHASEFLAG, 
            strm.INITIATEDBY, 
            strm.INITIATEDTTM, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFBILLSINRUN, 
            strm.NUMBEROFEXCEPTIONS, 
            strm.OUTPUTTEDFLAG, 
            strm.OUTPUTTINGFLAG, 
            strm.POSTEDFLAG, 
            strm.POSTINGFLAG, 
            strm.PRESELECTEDFLAG, 
            strm.PRINCIPALTOTAL, 
            strm.PROCESSINGFLAG, 
            strm.REFRESHBILLBALANCESFLAG, 
            strm.REFRESHBILLMAILTOINFOFLAG, 
            strm.REVERSEDFLAG, 
            strm.REVERSEKEY, 
            strm.ROUTEKEY, 
            strm.RUNAVERAGEBILLSPERMINUTE, 
            strm.RUNTYPE, 
            strm.SCHEDULEKEY, 
            strm.TOTALNUMBEROFBILLS, 
            strm.VARIATION_ID, 
            strm.WASRESUMED, 
            strm.WASSECONDSTEPPOSTED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BILLRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLRUN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BILLRUNKEY=src.BILLRUNKEY) OR (target.BILLRUNKEY IS NULL AND src.BILLRUNKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTAREA=src.ACCOUNTAREA, 
                    target.ACCOUNTCLASS=src.ACCOUNTCLASS, 
                    target.ACCOUNTGROUP=src.ACCOUNTGROUP, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTSUBGROUP=src.ACCOUNTSUBGROUP, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLDATE=src.BILLDATE, 
                    target.BILLINGCYCLE=src.BILLINGCYCLE, 
                    target.BILLINGPERIODFROMDATE=src.BILLINGPERIODFROMDATE, 
                    target.BILLINGPERIODTODATE=src.BILLINGPERIODTODATE, 
                    target.BILLINGSTATUS=src.BILLINGSTATUS, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BILLRUNKEY=src.BILLRUNKEY, 
                    target.BILLRUNSCHEDULEKEY=src.BILLRUNSCHEDULEKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.CALCULATEDFLAG=src.CALCULATEDFLAG, 
                    target.CALCULATINGFLAG=src.CALCULATINGFLAG, 
                    target.CDRAPDEFNKEY=src.CDRAPDEFNKEY, 
                    target.CDRAPPROCESSSTATEKEY=src.CDRAPPROCESSSTATEKEY, 
                    target.CDRPRODFAMILY=src.CDRPRODFAMILY, 
                    target.COMMITNUMBER=src.COMMITNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.DUEDATE=src.DUEDATE, 
                    target.EXCLUDEFINALBILLSFLAG=src.EXCLUDEFINALBILLSFLAG, 
                    target.INCLUDESUBBILLTYPESFLAG=src.INCLUDESUBBILLTYPESFLAG, 
                    target.INITIALIZEPHASEFLAG=src.INITIALIZEPHASEFLAG, 
                    target.INITIATEDBY=src.INITIATEDBY, 
                    target.INITIATEDTTM=src.INITIATEDTTM, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFBILLSINRUN=src.NUMBEROFBILLSINRUN, 
                    target.NUMBEROFEXCEPTIONS=src.NUMBEROFEXCEPTIONS, 
                    target.OUTPUTTEDFLAG=src.OUTPUTTEDFLAG, 
                    target.OUTPUTTINGFLAG=src.OUTPUTTINGFLAG, 
                    target.POSTEDFLAG=src.POSTEDFLAG, 
                    target.POSTINGFLAG=src.POSTINGFLAG, 
                    target.PRESELECTEDFLAG=src.PRESELECTEDFLAG, 
                    target.PRINCIPALTOTAL=src.PRINCIPALTOTAL, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.REFRESHBILLBALANCESFLAG=src.REFRESHBILLBALANCESFLAG, 
                    target.REFRESHBILLMAILTOINFOFLAG=src.REFRESHBILLMAILTOINFOFLAG, 
                    target.REVERSEDFLAG=src.REVERSEDFLAG, 
                    target.REVERSEKEY=src.REVERSEKEY, 
                    target.ROUTEKEY=src.ROUTEKEY, 
                    target.RUNAVERAGEBILLSPERMINUTE=src.RUNAVERAGEBILLSPERMINUTE, 
                    target.RUNTYPE=src.RUNTYPE, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.TOTALNUMBEROFBILLS=src.TOTALNUMBEROFBILLS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WASRESUMED=src.WASRESUMED, 
                    target.WASSECONDSTEPPOSTED=src.WASSECONDSTEPPOSTED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTAREA, 
                    ACCOUNTCLASS, 
                    ACCOUNTGROUP, 
                    ACCOUNTKEY, 
                    ACCOUNTSUBGROUP, 
                    ADDBY, 
                    ADDDTTM, 
                    BILLDATE, 
                    BILLINGCYCLE, 
                    BILLINGPERIODFROMDATE, 
                    BILLINGPERIODTODATE, 
                    BILLINGSTATUS, 
                    BILLKEY, 
                    BILLRUNKEY, 
                    BILLRUNSCHEDULEKEY, 
                    BILLTYPEKEY, 
                    CALCULATEDFLAG, 
                    CALCULATINGFLAG, 
                    CDRAPDEFNKEY, 
                    CDRAPPROCESSSTATEKEY, 
                    CDRPRODFAMILY, 
                    COMMITNUMBER, 
                    DELETED, 
                    DESCRIPTION, 
                    DUEDATE, 
                    EXCLUDEFINALBILLSFLAG, 
                    INCLUDESUBBILLTYPESFLAG, 
                    INITIALIZEPHASEFLAG, 
                    INITIATEDBY, 
                    INITIATEDTTM, 
                    LASTINVOCATIONSTATUS, 
                    MODBY, 
                    MODDTTM, 
                    NUMBEROFBILLSINRUN, 
                    NUMBEROFEXCEPTIONS, 
                    OUTPUTTEDFLAG, 
                    OUTPUTTINGFLAG, 
                    POSTEDFLAG, 
                    POSTINGFLAG, 
                    PRESELECTEDFLAG, 
                    PRINCIPALTOTAL, 
                    PROCESSINGFLAG, 
                    REFRESHBILLBALANCESFLAG, 
                    REFRESHBILLMAILTOINFOFLAG, 
                    REVERSEDFLAG, 
                    REVERSEKEY, 
                    ROUTEKEY, 
                    RUNAVERAGEBILLSPERMINUTE, 
                    RUNTYPE, 
                    SCHEDULEKEY, 
                    TOTALNUMBEROFBILLS, 
                    VARIATION_ID, 
                    WASRESUMED, 
                    WASSECONDSTEPPOSTED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTAREA, 
                    src.ACCOUNTCLASS, 
                    src.ACCOUNTGROUP, 
                    src.ACCOUNTKEY, 
                    src.ACCOUNTSUBGROUP, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLDATE, 
                    src.BILLINGCYCLE, 
                    src.BILLINGPERIODFROMDATE, 
                    src.BILLINGPERIODTODATE, 
                    src.BILLINGSTATUS, 
                    src.BILLKEY, 
                    src.BILLRUNKEY, 
                    src.BILLRUNSCHEDULEKEY, 
                    src.BILLTYPEKEY, 
                    src.CALCULATEDFLAG, 
                    src.CALCULATINGFLAG, 
                    src.CDRAPDEFNKEY, 
                    src.CDRAPPROCESSSTATEKEY, 
                    src.CDRPRODFAMILY, 
                    src.COMMITNUMBER, 
                    src.DELETED, 
                    src.DESCRIPTION, 
                    src.DUEDATE, 
                    src.EXCLUDEFINALBILLSFLAG, 
                    src.INCLUDESUBBILLTYPESFLAG, 
                    src.INITIALIZEPHASEFLAG, 
                    src.INITIATEDBY, 
                    src.INITIATEDTTM, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFBILLSINRUN, 
                    src.NUMBEROFEXCEPTIONS, 
                    src.OUTPUTTEDFLAG, 
                    src.OUTPUTTINGFLAG, 
                    src.POSTEDFLAG, 
                    src.POSTINGFLAG, 
                    src.PRESELECTEDFLAG, 
                    src.PRINCIPALTOTAL, 
                    src.PROCESSINGFLAG, 
                    src.REFRESHBILLBALANCESFLAG, 
                    src.REFRESHBILLMAILTOINFOFLAG, 
                    src.REVERSEDFLAG, 
                    src.REVERSEKEY, 
                    src.ROUTEKEY, 
                    src.RUNAVERAGEBILLSPERMINUTE, 
                    src.RUNTYPE, 
                    src.SCHEDULEKEY, 
                    src.TOTALNUMBEROFBILLS, 
                    src.VARIATION_ID, 
                    src.WASRESUMED, 
                    src.WASSECONDSTEPPOSTED,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BILLRUN as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTAREA, 
            strm.ACCOUNTCLASS, 
            strm.ACCOUNTGROUP, 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTSUBGROUP, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLDATE, 
            strm.BILLINGCYCLE, 
            strm.BILLINGPERIODFROMDATE, 
            strm.BILLINGPERIODTODATE, 
            strm.BILLINGSTATUS, 
            strm.BILLKEY, 
            strm.BILLRUNKEY, 
            strm.BILLRUNSCHEDULEKEY, 
            strm.BILLTYPEKEY, 
            strm.CALCULATEDFLAG, 
            strm.CALCULATINGFLAG, 
            strm.CDRAPDEFNKEY, 
            strm.CDRAPPROCESSSTATEKEY, 
            strm.CDRPRODFAMILY, 
            strm.COMMITNUMBER, 
            strm.DELETED, 
            strm.DESCRIPTION, 
            strm.DUEDATE, 
            strm.EXCLUDEFINALBILLSFLAG, 
            strm.INCLUDESUBBILLTYPESFLAG, 
            strm.INITIALIZEPHASEFLAG, 
            strm.INITIATEDBY, 
            strm.INITIATEDTTM, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFBILLSINRUN, 
            strm.NUMBEROFEXCEPTIONS, 
            strm.OUTPUTTEDFLAG, 
            strm.OUTPUTTINGFLAG, 
            strm.POSTEDFLAG, 
            strm.POSTINGFLAG, 
            strm.PRESELECTEDFLAG, 
            strm.PRINCIPALTOTAL, 
            strm.PROCESSINGFLAG, 
            strm.REFRESHBILLBALANCESFLAG, 
            strm.REFRESHBILLMAILTOINFOFLAG, 
            strm.REVERSEDFLAG, 
            strm.REVERSEKEY, 
            strm.ROUTEKEY, 
            strm.RUNAVERAGEBILLSPERMINUTE, 
            strm.RUNTYPE, 
            strm.SCHEDULEKEY, 
            strm.TOTALNUMBEROFBILLS, 
            strm.VARIATION_ID, 
            strm.WASRESUMED, 
            strm.WASSECONDSTEPPOSTED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BILLRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLRUN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BILLRUNKEY=src.BILLRUNKEY) OR (target.BILLRUNKEY IS NULL AND src.BILLRUNKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTAREA=src.ACCOUNTAREA, 
                    target.ACCOUNTCLASS=src.ACCOUNTCLASS, 
                    target.ACCOUNTGROUP=src.ACCOUNTGROUP, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTSUBGROUP=src.ACCOUNTSUBGROUP, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLDATE=src.BILLDATE, 
                    target.BILLINGCYCLE=src.BILLINGCYCLE, 
                    target.BILLINGPERIODFROMDATE=src.BILLINGPERIODFROMDATE, 
                    target.BILLINGPERIODTODATE=src.BILLINGPERIODTODATE, 
                    target.BILLINGSTATUS=src.BILLINGSTATUS, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BILLRUNKEY=src.BILLRUNKEY, 
                    target.BILLRUNSCHEDULEKEY=src.BILLRUNSCHEDULEKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.CALCULATEDFLAG=src.CALCULATEDFLAG, 
                    target.CALCULATINGFLAG=src.CALCULATINGFLAG, 
                    target.CDRAPDEFNKEY=src.CDRAPDEFNKEY, 
                    target.CDRAPPROCESSSTATEKEY=src.CDRAPPROCESSSTATEKEY, 
                    target.CDRPRODFAMILY=src.CDRPRODFAMILY, 
                    target.COMMITNUMBER=src.COMMITNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.DUEDATE=src.DUEDATE, 
                    target.EXCLUDEFINALBILLSFLAG=src.EXCLUDEFINALBILLSFLAG, 
                    target.INCLUDESUBBILLTYPESFLAG=src.INCLUDESUBBILLTYPESFLAG, 
                    target.INITIALIZEPHASEFLAG=src.INITIALIZEPHASEFLAG, 
                    target.INITIATEDBY=src.INITIATEDBY, 
                    target.INITIATEDTTM=src.INITIATEDTTM, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFBILLSINRUN=src.NUMBEROFBILLSINRUN, 
                    target.NUMBEROFEXCEPTIONS=src.NUMBEROFEXCEPTIONS, 
                    target.OUTPUTTEDFLAG=src.OUTPUTTEDFLAG, 
                    target.OUTPUTTINGFLAG=src.OUTPUTTINGFLAG, 
                    target.POSTEDFLAG=src.POSTEDFLAG, 
                    target.POSTINGFLAG=src.POSTINGFLAG, 
                    target.PRESELECTEDFLAG=src.PRESELECTEDFLAG, 
                    target.PRINCIPALTOTAL=src.PRINCIPALTOTAL, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.REFRESHBILLBALANCESFLAG=src.REFRESHBILLBALANCESFLAG, 
                    target.REFRESHBILLMAILTOINFOFLAG=src.REFRESHBILLMAILTOINFOFLAG, 
                    target.REVERSEDFLAG=src.REVERSEDFLAG, 
                    target.REVERSEKEY=src.REVERSEKEY, 
                    target.ROUTEKEY=src.ROUTEKEY, 
                    target.RUNAVERAGEBILLSPERMINUTE=src.RUNAVERAGEBILLSPERMINUTE, 
                    target.RUNTYPE=src.RUNTYPE, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.TOTALNUMBEROFBILLS=src.TOTALNUMBEROFBILLS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WASRESUMED=src.WASRESUMED, 
                    target.WASSECONDSTEPPOSTED=src.WASSECONDSTEPPOSTED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTAREA, ACCOUNTCLASS, ACCOUNTGROUP, ACCOUNTKEY, ACCOUNTSUBGROUP, ADDBY, ADDDTTM, BILLDATE, BILLINGCYCLE, BILLINGPERIODFROMDATE, BILLINGPERIODTODATE, BILLINGSTATUS, BILLKEY, BILLRUNKEY, BILLRUNSCHEDULEKEY, BILLTYPEKEY, CALCULATEDFLAG, CALCULATINGFLAG, CDRAPDEFNKEY, CDRAPPROCESSSTATEKEY, CDRPRODFAMILY, COMMITNUMBER, DELETED, DESCRIPTION, DUEDATE, EXCLUDEFINALBILLSFLAG, INCLUDESUBBILLTYPESFLAG, INITIALIZEPHASEFLAG, INITIATEDBY, INITIATEDTTM, LASTINVOCATIONSTATUS, MODBY, MODDTTM, NUMBEROFBILLSINRUN, NUMBEROFEXCEPTIONS, OUTPUTTEDFLAG, OUTPUTTINGFLAG, POSTEDFLAG, POSTINGFLAG, PRESELECTEDFLAG, PRINCIPALTOTAL, PROCESSINGFLAG, REFRESHBILLBALANCESFLAG, REFRESHBILLMAILTOINFOFLAG, REVERSEDFLAG, REVERSEKEY, ROUTEKEY, RUNAVERAGEBILLSPERMINUTE, RUNTYPE, SCHEDULEKEY, TOTALNUMBEROFBILLS, VARIATION_ID, WASRESUMED, WASSECONDSTEPPOSTED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTAREA, 
                    src.ACCOUNTCLASS, 
                    src.ACCOUNTGROUP, 
                    src.ACCOUNTKEY, 
                    src.ACCOUNTSUBGROUP, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLDATE, 
                    src.BILLINGCYCLE, 
                    src.BILLINGPERIODFROMDATE, 
                    src.BILLINGPERIODTODATE, 
                    src.BILLINGSTATUS, 
                    src.BILLKEY, 
                    src.BILLRUNKEY, 
                    src.BILLRUNSCHEDULEKEY, 
                    src.BILLTYPEKEY, 
                    src.CALCULATEDFLAG, 
                    src.CALCULATINGFLAG, 
                    src.CDRAPDEFNKEY, 
                    src.CDRAPPROCESSSTATEKEY, 
                    src.CDRPRODFAMILY, 
                    src.COMMITNUMBER, 
                    src.DELETED, 
                    src.DESCRIPTION, 
                    src.DUEDATE, 
                    src.EXCLUDEFINALBILLSFLAG, 
                    src.INCLUDESUBBILLTYPESFLAG, 
                    src.INITIALIZEPHASEFLAG, 
                    src.INITIATEDBY, 
                    src.INITIATEDTTM, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFBILLSINRUN, 
                    src.NUMBEROFEXCEPTIONS, 
                    src.OUTPUTTEDFLAG, 
                    src.OUTPUTTINGFLAG, 
                    src.POSTEDFLAG, 
                    src.POSTINGFLAG, 
                    src.PRESELECTEDFLAG, 
                    src.PRINCIPALTOTAL, 
                    src.PROCESSINGFLAG, 
                    src.REFRESHBILLBALANCESFLAG, 
                    src.REFRESHBILLMAILTOINFOFLAG, 
                    src.REVERSEDFLAG, 
                    src.REVERSEKEY, 
                    src.ROUTEKEY, 
                    src.RUNAVERAGEBILLSPERMINUTE, 
                    src.RUNTYPE, 
                    src.SCHEDULEKEY, 
                    src.TOTALNUMBEROFBILLS, 
                    src.VARIATION_ID, 
                    src.WASRESUMED, 
                    src.WASSECONDSTEPPOSTED,     
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