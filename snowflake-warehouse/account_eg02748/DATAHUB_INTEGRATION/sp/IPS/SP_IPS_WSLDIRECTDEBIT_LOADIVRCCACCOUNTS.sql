create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.AUTHCODE, 
            strm.AUTHORISED, 
            strm.BILLINGID, 
            strm.CARDHOLDERNAME, 
            strm.CARDNO, 
            strm.CARDTYPE, 
            strm.CUR, 
            strm.DELETED, 
            strm.DPSBILLINGID, 
            strm.DPSTXNREF, 
            strm.FILEEXTENSION, 
            strm.FILENAME, 
            strm.GROUPACCT, 
            strm.LOADIVRCCACCOUNTSKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.REFERENCE, 
            strm.RESPCODE, 
            strm.RESPTEXT, 
            strm.SETTLEMENTDATE, 
            strm.STATUS, 
            strm.STATUSMESG, 
            strm.TIME, 
            strm.TXNDATA1, 
            strm.TXNDATA2, 
            strm.TXNDATA3, 
            strm.TXNREF, 
            strm.TYPE, 
            strm.USERNAME, 
            strm.VARIATION_ID, 
            strm.VOIDED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOADIVRCCACCOUNTSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOADIVRCCACCOUNTSKEY=src.LOADIVRCCACCOUNTSKEY) OR (target.LOADIVRCCACCOUNTSKEY IS NULL AND src.LOADIVRCCACCOUNTSKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.AUTHCODE=src.AUTHCODE, 
                    target.AUTHORISED=src.AUTHORISED, 
                    target.BILLINGID=src.BILLINGID, 
                    target.CARDHOLDERNAME=src.CARDHOLDERNAME, 
                    target.CARDNO=src.CARDNO, 
                    target.CARDTYPE=src.CARDTYPE, 
                    target.CUR=src.CUR, 
                    target.DELETED=src.DELETED, 
                    target.DPSBILLINGID=src.DPSBILLINGID, 
                    target.DPSTXNREF=src.DPSTXNREF, 
                    target.FILEEXTENSION=src.FILEEXTENSION, 
                    target.FILENAME=src.FILENAME, 
                    target.GROUPACCT=src.GROUPACCT, 
                    target.LOADIVRCCACCOUNTSKEY=src.LOADIVRCCACCOUNTSKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.REFERENCE=src.REFERENCE, 
                    target.RESPCODE=src.RESPCODE, 
                    target.RESPTEXT=src.RESPTEXT, 
                    target.SETTLEMENTDATE=src.SETTLEMENTDATE, 
                    target.STATUS=src.STATUS, 
                    target.STATUSMESG=src.STATUSMESG, 
                    target.TIME=src.TIME, 
                    target.TXNDATA1=src.TXNDATA1, 
                    target.TXNDATA2=src.TXNDATA2, 
                    target.TXNDATA3=src.TXNDATA3, 
                    target.TXNREF=src.TXNREF, 
                    target.TYPE=src.TYPE, 
                    target.USERNAME=src.USERNAME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VOIDED=src.VOIDED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    AMOUNT, 
                    AUTHCODE, 
                    AUTHORISED, 
                    BILLINGID, 
                    CARDHOLDERNAME, 
                    CARDNO, 
                    CARDTYPE, 
                    CUR, 
                    DELETED, 
                    DPSBILLINGID, 
                    DPSTXNREF, 
                    FILEEXTENSION, 
                    FILENAME, 
                    GROUPACCT, 
                    LOADIVRCCACCOUNTSKEY, 
                    MODBY, 
                    MODDTTM, 
                    REFERENCE, 
                    RESPCODE, 
                    RESPTEXT, 
                    SETTLEMENTDATE, 
                    STATUS, 
                    STATUSMESG, 
                    TIME, 
                    TXNDATA1, 
                    TXNDATA2, 
                    TXNDATA3, 
                    TXNREF, 
                    TYPE, 
                    USERNAME, 
                    VARIATION_ID, 
                    VOIDED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.AUTHCODE, 
                    src.AUTHORISED, 
                    src.BILLINGID, 
                    src.CARDHOLDERNAME, 
                    src.CARDNO, 
                    src.CARDTYPE, 
                    src.CUR, 
                    src.DELETED, 
                    src.DPSBILLINGID, 
                    src.DPSTXNREF, 
                    src.FILEEXTENSION, 
                    src.FILENAME, 
                    src.GROUPACCT, 
                    src.LOADIVRCCACCOUNTSKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.REFERENCE, 
                    src.RESPCODE, 
                    src.RESPTEXT, 
                    src.SETTLEMENTDATE, 
                    src.STATUS, 
                    src.STATUSMESG, 
                    src.TIME, 
                    src.TXNDATA1, 
                    src.TXNDATA2, 
                    src.TXNDATA3, 
                    src.TXNREF, 
                    src.TYPE, 
                    src.USERNAME, 
                    src.VARIATION_ID, 
                    src.VOIDED,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMOUNT, 
            strm.AUTHCODE, 
            strm.AUTHORISED, 
            strm.BILLINGID, 
            strm.CARDHOLDERNAME, 
            strm.CARDNO, 
            strm.CARDTYPE, 
            strm.CUR, 
            strm.DELETED, 
            strm.DPSBILLINGID, 
            strm.DPSTXNREF, 
            strm.FILEEXTENSION, 
            strm.FILENAME, 
            strm.GROUPACCT, 
            strm.LOADIVRCCACCOUNTSKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.REFERENCE, 
            strm.RESPCODE, 
            strm.RESPTEXT, 
            strm.SETTLEMENTDATE, 
            strm.STATUS, 
            strm.STATUSMESG, 
            strm.TIME, 
            strm.TXNDATA1, 
            strm.TXNDATA2, 
            strm.TXNDATA3, 
            strm.TXNREF, 
            strm.TYPE, 
            strm.USERNAME, 
            strm.VARIATION_ID, 
            strm.VOIDED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOADIVRCCACCOUNTSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOADIVRCCACCOUNTSKEY=src.LOADIVRCCACCOUNTSKEY) OR (target.LOADIVRCCACCOUNTSKEY IS NULL AND src.LOADIVRCCACCOUNTSKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMOUNT=src.AMOUNT, 
                    target.AUTHCODE=src.AUTHCODE, 
                    target.AUTHORISED=src.AUTHORISED, 
                    target.BILLINGID=src.BILLINGID, 
                    target.CARDHOLDERNAME=src.CARDHOLDERNAME, 
                    target.CARDNO=src.CARDNO, 
                    target.CARDTYPE=src.CARDTYPE, 
                    target.CUR=src.CUR, 
                    target.DELETED=src.DELETED, 
                    target.DPSBILLINGID=src.DPSBILLINGID, 
                    target.DPSTXNREF=src.DPSTXNREF, 
                    target.FILEEXTENSION=src.FILEEXTENSION, 
                    target.FILENAME=src.FILENAME, 
                    target.GROUPACCT=src.GROUPACCT, 
                    target.LOADIVRCCACCOUNTSKEY=src.LOADIVRCCACCOUNTSKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.REFERENCE=src.REFERENCE, 
                    target.RESPCODE=src.RESPCODE, 
                    target.RESPTEXT=src.RESPTEXT, 
                    target.SETTLEMENTDATE=src.SETTLEMENTDATE, 
                    target.STATUS=src.STATUS, 
                    target.STATUSMESG=src.STATUSMESG, 
                    target.TIME=src.TIME, 
                    target.TXNDATA1=src.TXNDATA1, 
                    target.TXNDATA2=src.TXNDATA2, 
                    target.TXNDATA3=src.TXNDATA3, 
                    target.TXNREF=src.TXNREF, 
                    target.TYPE=src.TYPE, 
                    target.USERNAME=src.USERNAME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VOIDED=src.VOIDED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ADDBY, ADDDTTM, AMOUNT, AUTHCODE, AUTHORISED, BILLINGID, CARDHOLDERNAME, CARDNO, CARDTYPE, CUR, DELETED, DPSBILLINGID, DPSTXNREF, FILEEXTENSION, FILENAME, GROUPACCT, LOADIVRCCACCOUNTSKEY, MODBY, MODDTTM, REFERENCE, RESPCODE, RESPTEXT, SETTLEMENTDATE, STATUS, STATUSMESG, TIME, TXNDATA1, TXNDATA2, TXNDATA3, TXNREF, TYPE, USERNAME, VARIATION_ID, VOIDED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMOUNT, 
                    src.AUTHCODE, 
                    src.AUTHORISED, 
                    src.BILLINGID, 
                    src.CARDHOLDERNAME, 
                    src.CARDNO, 
                    src.CARDTYPE, 
                    src.CUR, 
                    src.DELETED, 
                    src.DPSBILLINGID, 
                    src.DPSTXNREF, 
                    src.FILEEXTENSION, 
                    src.FILENAME, 
                    src.GROUPACCT, 
                    src.LOADIVRCCACCOUNTSKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.REFERENCE, 
                    src.RESPCODE, 
                    src.RESPTEXT, 
                    src.SETTLEMENTDATE, 
                    src.STATUS, 
                    src.STATUSMESG, 
                    src.TIME, 
                    src.TXNDATA1, 
                    src.TXNDATA2, 
                    src.TXNDATA3, 
                    src.TXNREF, 
                    src.TYPE, 
                    src.USERNAME, 
                    src.VARIATION_ID, 
                    src.VOIDED,     
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