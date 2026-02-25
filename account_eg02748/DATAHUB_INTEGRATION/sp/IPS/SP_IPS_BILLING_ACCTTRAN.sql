create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_ACCTTRAN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCTTRAN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_ACCTTRAN as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADJUSTMENTKEY, 
            strm.APPFEEKEY, 
            strm.COMMENTSKEY, 
            strm.DATALAKE_DELETED, 
            strm.DEPOSITTRANSACTIONKEY, 
            strm.INTERNALFLAG, 
            strm.LINEITEMKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEEDSJOURNAL, 
            strm.ONEOFFCHARGEKEY, 
            strm.PAYKEY, 
            strm.PENALTYKEY, 
            strm.REFTRANNO, 
            strm.REFUNDKEY, 
            strm.SOURCEKEY, 
            strm.SOURCETYPE, 
            strm.TRANAMT, 
            strm.TRANBY, 
            strm.TRANDATE, 
            strm.TRANNO, 
            strm.TRANSACTIONDESIGNATOR, 
            strm.TRANSACTIONTYPE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TRANNO ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCTTRAN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TRANNO=src.TRANNO) OR (target.TRANNO IS NULL AND src.TRANNO IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADJUSTMENTKEY=src.ADJUSTMENTKEY, 
                    target.APPFEEKEY=src.APPFEEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPOSITTRANSACTIONKEY=src.DEPOSITTRANSACTIONKEY, 
                    target.INTERNALFLAG=src.INTERNALFLAG, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEEDSJOURNAL=src.NEEDSJOURNAL, 
                    target.ONEOFFCHARGEKEY=src.ONEOFFCHARGEKEY, 
                    target.PAYKEY=src.PAYKEY, 
                    target.PENALTYKEY=src.PENALTYKEY, 
                    target.REFTRANNO=src.REFTRANNO, 
                    target.REFUNDKEY=src.REFUNDKEY, 
                    target.SOURCEKEY=src.SOURCEKEY, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.TRANAMT=src.TRANAMT, 
                    target.TRANBY=src.TRANBY, 
                    target.TRANDATE=src.TRANDATE, 
                    target.TRANNO=src.TRANNO, 
                    target.TRANSACTIONDESIGNATOR=src.TRANSACTIONDESIGNATOR, 
                    target.TRANSACTIONTYPE=src.TRANSACTIONTYPE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    ADJUSTMENTKEY, 
                    APPFEEKEY, 
                    COMMENTSKEY, 
                    DATALAKE_DELETED, 
                    DEPOSITTRANSACTIONKEY, 
                    INTERNALFLAG, 
                    LINEITEMKEY, 
                    MODBY, 
                    MODDTTM, 
                    NEEDSJOURNAL, 
                    ONEOFFCHARGEKEY, 
                    PAYKEY, 
                    PENALTYKEY, 
                    REFTRANNO, 
                    REFUNDKEY, 
                    SOURCEKEY, 
                    SOURCETYPE, 
                    TRANAMT, 
                    TRANBY, 
                    TRANDATE, 
                    TRANNO, 
                    TRANSACTIONDESIGNATOR, 
                    TRANSACTIONTYPE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADJUSTMENTKEY, 
                    src.APPFEEKEY, 
                    src.COMMENTSKEY, 
                    src.DATALAKE_DELETED, 
                    src.DEPOSITTRANSACTIONKEY, 
                    src.INTERNALFLAG, 
                    src.LINEITEMKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEEDSJOURNAL, 
                    src.ONEOFFCHARGEKEY, 
                    src.PAYKEY, 
                    src.PENALTYKEY, 
                    src.REFTRANNO, 
                    src.REFUNDKEY, 
                    src.SOURCEKEY, 
                    src.SOURCETYPE, 
                    src.TRANAMT, 
                    src.TRANBY, 
                    src.TRANDATE, 
                    src.TRANNO, 
                    src.TRANSACTIONDESIGNATOR, 
                    src.TRANSACTIONTYPE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_ACCTTRAN as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADJUSTMENTKEY, 
            strm.APPFEEKEY, 
            strm.COMMENTSKEY, 
            strm.DATALAKE_DELETED, 
            strm.DEPOSITTRANSACTIONKEY, 
            strm.INTERNALFLAG, 
            strm.LINEITEMKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEEDSJOURNAL, 
            strm.ONEOFFCHARGEKEY, 
            strm.PAYKEY, 
            strm.PENALTYKEY, 
            strm.REFTRANNO, 
            strm.REFUNDKEY, 
            strm.SOURCEKEY, 
            strm.SOURCETYPE, 
            strm.TRANAMT, 
            strm.TRANBY, 
            strm.TRANDATE, 
            strm.TRANNO, 
            strm.TRANSACTIONDESIGNATOR, 
            strm.TRANSACTIONTYPE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TRANNO ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCTTRAN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TRANNO=src.TRANNO) OR (target.TRANNO IS NULL AND src.TRANNO IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADJUSTMENTKEY=src.ADJUSTMENTKEY, 
                    target.APPFEEKEY=src.APPFEEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPOSITTRANSACTIONKEY=src.DEPOSITTRANSACTIONKEY, 
                    target.INTERNALFLAG=src.INTERNALFLAG, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEEDSJOURNAL=src.NEEDSJOURNAL, 
                    target.ONEOFFCHARGEKEY=src.ONEOFFCHARGEKEY, 
                    target.PAYKEY=src.PAYKEY, 
                    target.PENALTYKEY=src.PENALTYKEY, 
                    target.REFTRANNO=src.REFTRANNO, 
                    target.REFUNDKEY=src.REFUNDKEY, 
                    target.SOURCEKEY=src.SOURCEKEY, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.TRANAMT=src.TRANAMT, 
                    target.TRANBY=src.TRANBY, 
                    target.TRANDATE=src.TRANDATE, 
                    target.TRANNO=src.TRANNO, 
                    target.TRANSACTIONDESIGNATOR=src.TRANSACTIONDESIGNATOR, 
                    target.TRANSACTIONTYPE=src.TRANSACTIONTYPE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ADDBY, ADDDTTM, ADJUSTMENTKEY, APPFEEKEY, COMMENTSKEY, DATALAKE_DELETED, DEPOSITTRANSACTIONKEY, INTERNALFLAG, LINEITEMKEY, MODBY, MODDTTM, NEEDSJOURNAL, ONEOFFCHARGEKEY, PAYKEY, PENALTYKEY, REFTRANNO, REFUNDKEY, SOURCEKEY, SOURCETYPE, TRANAMT, TRANBY, TRANDATE, TRANNO, TRANSACTIONDESIGNATOR, TRANSACTIONTYPE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADJUSTMENTKEY, 
                    src.APPFEEKEY, 
                    src.COMMENTSKEY, 
                    src.DATALAKE_DELETED, 
                    src.DEPOSITTRANSACTIONKEY, 
                    src.INTERNALFLAG, 
                    src.LINEITEMKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEEDSJOURNAL, 
                    src.ONEOFFCHARGEKEY, 
                    src.PAYKEY, 
                    src.PENALTYKEY, 
                    src.REFTRANNO, 
                    src.REFUNDKEY, 
                    src.SOURCEKEY, 
                    src.SOURCETYPE, 
                    src.TRANAMT, 
                    src.TRANBY, 
                    src.TRANDATE, 
                    src.TRANNO, 
                    src.TRANSACTIONDESIGNATOR, 
                    src.TRANSACTIONTYPE, 
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