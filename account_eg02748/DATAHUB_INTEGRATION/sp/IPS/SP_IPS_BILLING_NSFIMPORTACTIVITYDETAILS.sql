create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_NSFIMPORTACTIVITYDETAILS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_NSFIMPORTACTIVITYDETAILS as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTIDENTIFIER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.AUTOFLAG, 
            strm.CASHERINGBASEDFLAG, 
            strm.CHECKNUMBER, 
            strm.DATAROW, 
            strm.DELETED, 
            strm.EMPLOYEE, 
            strm.EXCEPTIONDESC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTES, 
            strm.NSFCREATEDFLAG, 
            strm.NSFIMPORTACTIVITYDTLKEY, 
            strm.NSFIMPORTACTIVITYKEY, 
            strm.NSFPROCESSEXCEPTION, 
            strm.PAYMENTBATCHKEY, 
            strm.PAYMENTIDENTIFIER, 
            strm.RERUNFLAG, 
            strm.RESOLVEDFLAG, 
            strm.RETURNREASONCODE, 
            strm.REVERSEDFLAG, 
            strm.REVIEWDATE, 
            strm.REVIEWEDFLAG, 
            strm.TRANSACTIONDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NSFIMPORTACTIVITYDTLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NSFIMPORTACTIVITYDTLKEY=src.NSFIMPORTACTIVITYDTLKEY) OR (target.NSFIMPORTACTIVITYDTLKEY IS NULL AND src.NSFIMPORTACTIVITYDTLKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTIDENTIFIER=src.ACCOUNTIDENTIFIER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.AUTOFLAG=src.AUTOFLAG, 
                    target.CASHERINGBASEDFLAG=src.CASHERINGBASEDFLAG, 
                    target.CHECKNUMBER=src.CHECKNUMBER, 
                    target.DATAROW=src.DATAROW, 
                    target.DELETED=src.DELETED, 
                    target.EMPLOYEE=src.EMPLOYEE, 
                    target.EXCEPTIONDESC=src.EXCEPTIONDESC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTES=src.NOTES, 
                    target.NSFCREATEDFLAG=src.NSFCREATEDFLAG, 
                    target.NSFIMPORTACTIVITYDTLKEY=src.NSFIMPORTACTIVITYDTLKEY, 
                    target.NSFIMPORTACTIVITYKEY=src.NSFIMPORTACTIVITYKEY, 
                    target.NSFPROCESSEXCEPTION=src.NSFPROCESSEXCEPTION, 
                    target.PAYMENTBATCHKEY=src.PAYMENTBATCHKEY, 
                    target.PAYMENTIDENTIFIER=src.PAYMENTIDENTIFIER, 
                    target.RERUNFLAG=src.RERUNFLAG, 
                    target.RESOLVEDFLAG=src.RESOLVEDFLAG, 
                    target.RETURNREASONCODE=src.RETURNREASONCODE, 
                    target.REVERSEDFLAG=src.REVERSEDFLAG, 
                    target.REVIEWDATE=src.REVIEWDATE, 
                    target.REVIEWEDFLAG=src.REVIEWEDFLAG, 
                    target.TRANSACTIONDATE=src.TRANSACTIONDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTIDENTIFIER, 
                    ADDBY, 
                    ADDDTTM, 
                    AMOUNT, 
                    AUTOFLAG, 
                    CASHERINGBASEDFLAG, 
                    CHECKNUMBER, 
                    DATAROW, 
                    DELETED, 
                    EMPLOYEE, 
                    EXCEPTIONDESC, 
                    MODBY, 
                    MODDTTM, 
                    NOTES, 
                    NSFCREATEDFLAG, 
                    NSFIMPORTACTIVITYDTLKEY, 
                    NSFIMPORTACTIVITYKEY, 
                    NSFPROCESSEXCEPTION, 
                    PAYMENTBATCHKEY, 
                    PAYMENTIDENTIFIER, 
                    RERUNFLAG, 
                    RESOLVEDFLAG, 
                    RETURNREASONCODE, 
                    REVERSEDFLAG, 
                    REVIEWDATE, 
                    REVIEWEDFLAG, 
                    TRANSACTIONDATE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTIDENTIFIER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.AUTOFLAG, 
                    src.CASHERINGBASEDFLAG, 
                    src.CHECKNUMBER, 
                    src.DATAROW, 
                    src.DELETED, 
                    src.EMPLOYEE, 
                    src.EXCEPTIONDESC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTES, 
                    src.NSFCREATEDFLAG, 
                    src.NSFIMPORTACTIVITYDTLKEY, 
                    src.NSFIMPORTACTIVITYKEY, 
                    src.NSFPROCESSEXCEPTION, 
                    src.PAYMENTBATCHKEY, 
                    src.PAYMENTIDENTIFIER, 
                    src.RERUNFLAG, 
                    src.RESOLVEDFLAG, 
                    src.RETURNREASONCODE, 
                    src.REVERSEDFLAG, 
                    src.REVIEWDATE, 
                    src.REVIEWEDFLAG, 
                    src.TRANSACTIONDATE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_NSFIMPORTACTIVITYDETAILS as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTIDENTIFIER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.AUTOFLAG, 
            strm.CASHERINGBASEDFLAG, 
            strm.CHECKNUMBER, 
            strm.DATAROW, 
            strm.DELETED, 
            strm.EMPLOYEE, 
            strm.EXCEPTIONDESC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTES, 
            strm.NSFCREATEDFLAG, 
            strm.NSFIMPORTACTIVITYDTLKEY, 
            strm.NSFIMPORTACTIVITYKEY, 
            strm.NSFPROCESSEXCEPTION, 
            strm.PAYMENTBATCHKEY, 
            strm.PAYMENTIDENTIFIER, 
            strm.RERUNFLAG, 
            strm.RESOLVEDFLAG, 
            strm.RETURNREASONCODE, 
            strm.REVERSEDFLAG, 
            strm.REVIEWDATE, 
            strm.REVIEWEDFLAG, 
            strm.TRANSACTIONDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NSFIMPORTACTIVITYDTLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NSFIMPORTACTIVITYDTLKEY=src.NSFIMPORTACTIVITYDTLKEY) OR (target.NSFIMPORTACTIVITYDTLKEY IS NULL AND src.NSFIMPORTACTIVITYDTLKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTIDENTIFIER=src.ACCOUNTIDENTIFIER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.AUTOFLAG=src.AUTOFLAG, 
                    target.CASHERINGBASEDFLAG=src.CASHERINGBASEDFLAG, 
                    target.CHECKNUMBER=src.CHECKNUMBER, 
                    target.DATAROW=src.DATAROW, 
                    target.DELETED=src.DELETED, 
                    target.EMPLOYEE=src.EMPLOYEE, 
                    target.EXCEPTIONDESC=src.EXCEPTIONDESC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTES=src.NOTES, 
                    target.NSFCREATEDFLAG=src.NSFCREATEDFLAG, 
                    target.NSFIMPORTACTIVITYDTLKEY=src.NSFIMPORTACTIVITYDTLKEY, 
                    target.NSFIMPORTACTIVITYKEY=src.NSFIMPORTACTIVITYKEY, 
                    target.NSFPROCESSEXCEPTION=src.NSFPROCESSEXCEPTION, 
                    target.PAYMENTBATCHKEY=src.PAYMENTBATCHKEY, 
                    target.PAYMENTIDENTIFIER=src.PAYMENTIDENTIFIER, 
                    target.RERUNFLAG=src.RERUNFLAG, 
                    target.RESOLVEDFLAG=src.RESOLVEDFLAG, 
                    target.RETURNREASONCODE=src.RETURNREASONCODE, 
                    target.REVERSEDFLAG=src.REVERSEDFLAG, 
                    target.REVIEWDATE=src.REVIEWDATE, 
                    target.REVIEWEDFLAG=src.REVIEWEDFLAG, 
                    target.TRANSACTIONDATE=src.TRANSACTIONDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTIDENTIFIER, ADDBY, ADDDTTM, AMOUNT, AUTOFLAG, CASHERINGBASEDFLAG, CHECKNUMBER, DATAROW, DELETED, EMPLOYEE, EXCEPTIONDESC, MODBY, MODDTTM, NOTES, NSFCREATEDFLAG, NSFIMPORTACTIVITYDTLKEY, NSFIMPORTACTIVITYKEY, NSFPROCESSEXCEPTION, PAYMENTBATCHKEY, PAYMENTIDENTIFIER, RERUNFLAG, RESOLVEDFLAG, RETURNREASONCODE, REVERSEDFLAG, REVIEWDATE, REVIEWEDFLAG, TRANSACTIONDATE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTIDENTIFIER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.AUTOFLAG, 
                    src.CASHERINGBASEDFLAG, 
                    src.CHECKNUMBER, 
                    src.DATAROW, 
                    src.DELETED, 
                    src.EMPLOYEE, 
                    src.EXCEPTIONDESC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTES, 
                    src.NSFCREATEDFLAG, 
                    src.NSFIMPORTACTIVITYDTLKEY, 
                    src.NSFIMPORTACTIVITYKEY, 
                    src.NSFPROCESSEXCEPTION, 
                    src.PAYMENTBATCHKEY, 
                    src.PAYMENTIDENTIFIER, 
                    src.RERUNFLAG, 
                    src.RESOLVEDFLAG, 
                    src.RETURNREASONCODE, 
                    src.REVERSEDFLAG, 
                    src.REVIEWDATE, 
                    src.REVIEWEDFLAG, 
                    src.TRANSACTIONDATE, 
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