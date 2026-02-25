create or replace procedure DATAHUB_INTEGRATION.SP_IPS_RESOURCES_JOURNALSUMMARY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_RESOURCES_JOURNALSUMMARY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_RESOURCES_JOURNALSUMMARY as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTINGPERIOD, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.CREDITBUDGETNUMBER, 
            strm.DEBITBUDGETNUMBER, 
            strm.DELETED, 
            strm.FISCALYEAR, 
            strm.FROMDATETIME, 
            strm.JOURNALSUMMARYKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.TODATETIME, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JOURNALSUMMARYKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_JOURNALSUMMARY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JOURNALSUMMARYKEY=src.JOURNALSUMMARYKEY) OR (target.JOURNALSUMMARYKEY IS NULL AND src.JOURNALSUMMARYKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTINGPERIOD=src.ACCOUNTINGPERIOD, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.CREDITBUDGETNUMBER=src.CREDITBUDGETNUMBER, 
                    target.DEBITBUDGETNUMBER=src.DEBITBUDGETNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.FISCALYEAR=src.FISCALYEAR, 
                    target.FROMDATETIME=src.FROMDATETIME, 
                    target.JOURNALSUMMARYKEY=src.JOURNALSUMMARYKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.TODATETIME=src.TODATETIME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTINGPERIOD, 
                    ADDBY, 
                    ADDDTTM, 
                    AMOUNT, 
                    CREDITBUDGETNUMBER, 
                    DEBITBUDGETNUMBER, 
                    DELETED, 
                    FISCALYEAR, 
                    FROMDATETIME, 
                    JOURNALSUMMARYKEY, 
                    MODBY, 
                    MODDTTM, 
                    TODATETIME, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTINGPERIOD, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.CREDITBUDGETNUMBER, 
                    src.DEBITBUDGETNUMBER, 
                    src.DELETED, 
                    src.FISCALYEAR, 
                    src.FROMDATETIME, 
                    src.JOURNALSUMMARYKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.TODATETIME, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_JOURNALSUMMARY as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTINGPERIOD, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.CREDITBUDGETNUMBER, 
            strm.DEBITBUDGETNUMBER, 
            strm.DELETED, 
            strm.FISCALYEAR, 
            strm.FROMDATETIME, 
            strm.JOURNALSUMMARYKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.TODATETIME, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JOURNALSUMMARYKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_JOURNALSUMMARY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JOURNALSUMMARYKEY=src.JOURNALSUMMARYKEY) OR (target.JOURNALSUMMARYKEY IS NULL AND src.JOURNALSUMMARYKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTINGPERIOD=src.ACCOUNTINGPERIOD, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.CREDITBUDGETNUMBER=src.CREDITBUDGETNUMBER, 
                    target.DEBITBUDGETNUMBER=src.DEBITBUDGETNUMBER, 
                    target.DELETED=src.DELETED, 
                    target.FISCALYEAR=src.FISCALYEAR, 
                    target.FROMDATETIME=src.FROMDATETIME, 
                    target.JOURNALSUMMARYKEY=src.JOURNALSUMMARYKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.TODATETIME=src.TODATETIME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTINGPERIOD, ADDBY, ADDDTTM, AMOUNT, CREDITBUDGETNUMBER, DEBITBUDGETNUMBER, DELETED, FISCALYEAR, FROMDATETIME, JOURNALSUMMARYKEY, MODBY, MODDTTM, TODATETIME, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTINGPERIOD, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.CREDITBUDGETNUMBER, 
                    src.DEBITBUDGETNUMBER, 
                    src.DELETED, 
                    src.FISCALYEAR, 
                    src.FROMDATETIME, 
                    src.JOURNALSUMMARYKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.TODATETIME, 
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