create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLBILLING_LOADPAYMENT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_LOADPAYMENT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLBILLING_LOADPAYMENT as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTNUMBER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.BATCHKEY, 
            strm.DELETED, 
            strm.FILEEXTENSION, 
            strm.FILENAME, 
            strm.LOADPAYMENTKEY, 
            strm.METHOD, 
            strm.MISCREFNO, 
            strm.MISCTYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OTHER1, 
            strm.OTHER2, 
            strm.OTHER3, 
            strm.PAYMENTDATE, 
            strm.RECORDCOUNT, 
            strm.RECORDTYPE, 
            strm.SOURCETYPE, 
            strm.STATUS, 
            strm.STATUSMESG, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOADPAYMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_LOADPAYMENT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOADPAYMENTKEY=src.LOADPAYMENTKEY) OR (target.LOADPAYMENTKEY IS NULL AND src.LOADPAYMENTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTNUMBER=src.ACCOUNTNUMBER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.BATCHKEY=src.BATCHKEY, 
                    target.DELETED=src.DELETED, 
                    target.FILEEXTENSION=src.FILEEXTENSION, 
                    target.FILENAME=src.FILENAME, 
                    target.LOADPAYMENTKEY=src.LOADPAYMENTKEY, 
                    target.METHOD=src.METHOD, 
                    target.MISCREFNO=src.MISCREFNO, 
                    target.MISCTYPE=src.MISCTYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OTHER1=src.OTHER1, 
                    target.OTHER2=src.OTHER2, 
                    target.OTHER3=src.OTHER3, 
                    target.PAYMENTDATE=src.PAYMENTDATE, 
                    target.RECORDCOUNT=src.RECORDCOUNT, 
                    target.RECORDTYPE=src.RECORDTYPE, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.STATUS=src.STATUS, 
                    target.STATUSMESG=src.STATUSMESG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTNUMBER, 
                    ADDBY, 
                    ADDDTTM, 
                    AMOUNT, 
                    BATCHKEY, 
                    DELETED, 
                    FILEEXTENSION, 
                    FILENAME, 
                    LOADPAYMENTKEY, 
                    METHOD, 
                    MISCREFNO, 
                    MISCTYPE, 
                    MODBY, 
                    MODDTTM, 
                    OTHER1, 
                    OTHER2, 
                    OTHER3, 
                    PAYMENTDATE, 
                    RECORDCOUNT, 
                    RECORDTYPE, 
                    SOURCETYPE, 
                    STATUS, 
                    STATUSMESG, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTNUMBER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.BATCHKEY, 
                    src.DELETED, 
                    src.FILEEXTENSION, 
                    src.FILENAME, 
                    src.LOADPAYMENTKEY, 
                    src.METHOD, 
                    src.MISCREFNO, 
                    src.MISCTYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OTHER1, 
                    src.OTHER2, 
                    src.OTHER3, 
                    src.PAYMENTDATE, 
                    src.RECORDCOUNT, 
                    src.RECORDTYPE, 
                    src.SOURCETYPE, 
                    src.STATUS, 
                    src.STATUSMESG, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLBILLING_LOADPAYMENT as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTNUMBER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.BATCHKEY, 
            strm.DELETED, 
            strm.FILEEXTENSION, 
            strm.FILENAME, 
            strm.LOADPAYMENTKEY, 
            strm.METHOD, 
            strm.MISCREFNO, 
            strm.MISCTYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OTHER1, 
            strm.OTHER2, 
            strm.OTHER3, 
            strm.PAYMENTDATE, 
            strm.RECORDCOUNT, 
            strm.RECORDTYPE, 
            strm.SOURCETYPE, 
            strm.STATUS, 
            strm.STATUSMESG, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOADPAYMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_LOADPAYMENT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOADPAYMENTKEY=src.LOADPAYMENTKEY) OR (target.LOADPAYMENTKEY IS NULL AND src.LOADPAYMENTKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTNUMBER=src.ACCOUNTNUMBER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.BATCHKEY=src.BATCHKEY, 
                    target.DELETED=src.DELETED, 
                    target.FILEEXTENSION=src.FILEEXTENSION, 
                    target.FILENAME=src.FILENAME, 
                    target.LOADPAYMENTKEY=src.LOADPAYMENTKEY, 
                    target.METHOD=src.METHOD, 
                    target.MISCREFNO=src.MISCREFNO, 
                    target.MISCTYPE=src.MISCTYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OTHER1=src.OTHER1, 
                    target.OTHER2=src.OTHER2, 
                    target.OTHER3=src.OTHER3, 
                    target.PAYMENTDATE=src.PAYMENTDATE, 
                    target.RECORDCOUNT=src.RECORDCOUNT, 
                    target.RECORDTYPE=src.RECORDTYPE, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.STATUS=src.STATUS, 
                    target.STATUSMESG=src.STATUSMESG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTNUMBER, ADDBY, ADDDTTM, AMOUNT, BATCHKEY, DELETED, FILEEXTENSION, FILENAME, LOADPAYMENTKEY, METHOD, MISCREFNO, MISCTYPE, MODBY, MODDTTM, OTHER1, OTHER2, OTHER3, PAYMENTDATE, RECORDCOUNT, RECORDTYPE, SOURCETYPE, STATUS, STATUSMESG, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTNUMBER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.BATCHKEY, 
                    src.DELETED, 
                    src.FILEEXTENSION, 
                    src.FILENAME, 
                    src.LOADPAYMENTKEY, 
                    src.METHOD, 
                    src.MISCREFNO, 
                    src.MISCTYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OTHER1, 
                    src.OTHER2, 
                    src.OTHER3, 
                    src.PAYMENTDATE, 
                    src.RECORDCOUNT, 
                    src.RECORDTYPE, 
                    src.SOURCETYPE, 
                    src.STATUS, 
                    src.STATUSMESG, 
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