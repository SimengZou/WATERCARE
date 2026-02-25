create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_PAYSPECIFICBILL()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYSPECIFICBILL_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_PAYSPECIFICBILL as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADJUSTMENTKEY, 
            strm.BILLKEY, 
            strm.CDRAPPLICATIONFEEKEY, 
            strm.DELETED, 
            strm.DEPOSITADJUSTMENTKEY, 
            strm.DEPOSITCHARGEKEY, 
            strm.LINEITEMKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ONEOFFCHARGEKEY, 
            strm.ORIGPAYALLOCAMT, 
            strm.PAYMENTKEY, 
            strm.PAYORDER, 
            strm.PAYSPECIFICBILLKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PAYSPECIFICBILLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYSPECIFICBILL as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PAYSPECIFICBILLKEY=src.PAYSPECIFICBILLKEY) OR (target.PAYSPECIFICBILLKEY IS NULL AND src.PAYSPECIFICBILLKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADJUSTMENTKEY=src.ADJUSTMENTKEY, 
                    target.BILLKEY=src.BILLKEY, 
                    target.CDRAPPLICATIONFEEKEY=src.CDRAPPLICATIONFEEKEY, 
                    target.DELETED=src.DELETED, 
                    target.DEPOSITADJUSTMENTKEY=src.DEPOSITADJUSTMENTKEY, 
                    target.DEPOSITCHARGEKEY=src.DEPOSITCHARGEKEY, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ONEOFFCHARGEKEY=src.ONEOFFCHARGEKEY, 
                    target.ORIGPAYALLOCAMT=src.ORIGPAYALLOCAMT, 
                    target.PAYMENTKEY=src.PAYMENTKEY, 
                    target.PAYORDER=src.PAYORDER, 
                    target.PAYSPECIFICBILLKEY=src.PAYSPECIFICBILLKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ADJUSTMENTKEY, 
                    BILLKEY, 
                    CDRAPPLICATIONFEEKEY, 
                    DELETED, 
                    DEPOSITADJUSTMENTKEY, 
                    DEPOSITCHARGEKEY, 
                    LINEITEMKEY, 
                    MODBY, 
                    MODDTTM, 
                    ONEOFFCHARGEKEY, 
                    ORIGPAYALLOCAMT, 
                    PAYMENTKEY, 
                    PAYORDER, 
                    PAYSPECIFICBILLKEY, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADJUSTMENTKEY, 
                    src.BILLKEY, 
                    src.CDRAPPLICATIONFEEKEY, 
                    src.DELETED, 
                    src.DEPOSITADJUSTMENTKEY, 
                    src.DEPOSITCHARGEKEY, 
                    src.LINEITEMKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ONEOFFCHARGEKEY, 
                    src.ORIGPAYALLOCAMT, 
                    src.PAYMENTKEY, 
                    src.PAYORDER, 
                    src.PAYSPECIFICBILLKEY, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_PAYSPECIFICBILL as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADJUSTMENTKEY, 
            strm.BILLKEY, 
            strm.CDRAPPLICATIONFEEKEY, 
            strm.DELETED, 
            strm.DEPOSITADJUSTMENTKEY, 
            strm.DEPOSITCHARGEKEY, 
            strm.LINEITEMKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ONEOFFCHARGEKEY, 
            strm.ORIGPAYALLOCAMT, 
            strm.PAYMENTKEY, 
            strm.PAYORDER, 
            strm.PAYSPECIFICBILLKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PAYSPECIFICBILLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYSPECIFICBILL as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PAYSPECIFICBILLKEY=src.PAYSPECIFICBILLKEY) OR (target.PAYSPECIFICBILLKEY IS NULL AND src.PAYSPECIFICBILLKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADJUSTMENTKEY=src.ADJUSTMENTKEY, 
                    target.BILLKEY=src.BILLKEY, 
                    target.CDRAPPLICATIONFEEKEY=src.CDRAPPLICATIONFEEKEY, 
                    target.DELETED=src.DELETED, 
                    target.DEPOSITADJUSTMENTKEY=src.DEPOSITADJUSTMENTKEY, 
                    target.DEPOSITCHARGEKEY=src.DEPOSITCHARGEKEY, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ONEOFFCHARGEKEY=src.ONEOFFCHARGEKEY, 
                    target.ORIGPAYALLOCAMT=src.ORIGPAYALLOCAMT, 
                    target.PAYMENTKEY=src.PAYMENTKEY, 
                    target.PAYORDER=src.PAYORDER, 
                    target.PAYSPECIFICBILLKEY=src.PAYSPECIFICBILLKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ADJUSTMENTKEY, BILLKEY, CDRAPPLICATIONFEEKEY, DELETED, DEPOSITADJUSTMENTKEY, DEPOSITCHARGEKEY, LINEITEMKEY, MODBY, MODDTTM, ONEOFFCHARGEKEY, ORIGPAYALLOCAMT, PAYMENTKEY, PAYORDER, PAYSPECIFICBILLKEY, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADJUSTMENTKEY, 
                    src.BILLKEY, 
                    src.CDRAPPLICATIONFEEKEY, 
                    src.DELETED, 
                    src.DEPOSITADJUSTMENTKEY, 
                    src.DEPOSITCHARGEKEY, 
                    src.LINEITEMKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ONEOFFCHARGEKEY, 
                    src.ORIGPAYALLOCAMT, 
                    src.PAYMENTKEY, 
                    src.PAYORDER, 
                    src.PAYSPECIFICBILLKEY, 
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