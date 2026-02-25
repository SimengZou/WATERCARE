create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CASHIERING_MERCHANTACCOUNT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CASHIERING_MERCHANTACCOUNT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CASHIERING_MERCHANTACCOUNT as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTID, 
            strm.ACCOUNTNAME, 
            strm.ACCOUNTPASSWORD, 
            strm.ACCOUNTUSERNAME, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTHHANDLERKEY, 
            strm.AUTHORIZECONFIGKEY, 
            strm.AUTHORIZEPAYMENTFORMULAKEY, 
            strm.CARDNUMBERFORMAT, 
            strm.CARDNUMBERFORMATOVR, 
            strm.DATALAKE_DELETED, 
            strm.DESCRIPTION, 
            strm.EFFECTIVEDATE, 
            strm.ENCRYPTCUSTPROP, 
            strm.EXPIREDATE, 
            strm.HOSTADDR, 
            strm.HOSTPORT, 
            strm.INQCONFIGKEY, 
            strm.INQHANDLERKEY, 
            strm.INQREVERSEDCONFIGKEY, 
            strm.INQREVERSEDHANDLERKEY, 
            strm.ISCHECKING, 
            strm.ISCREDIT, 
            strm.ISDEBIT, 
            strm.ISECOMMERCE, 
            strm.MERCHANTACCOUNTKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYMENTGATEWAYPROVIDER, 
            strm.PAYMENTPROCESSING, 
            strm.PREAUTHHANDLERKEY, 
            strm.PREAUTHORIZECONFIGKEY, 
            strm.PREPCONFIGKEY, 
            strm.PREPHANDLERKEY, 
            strm.REDIRECTURLCONFIGKEY, 
            strm.REDIRECTURLHANDLERKEY, 
            strm.REVERSECONFIGKEY, 
            strm.REVERSEHANDLERKEY, 
            strm.REVERSEPAYMENTFORMULAKEY, 
            strm.TIMEOUT, 
            strm.VARIATION_ID, 
            strm.VOIDAPPROVEDTRANSACTIONONLY, 
            strm.VOIDCONFIGKEY, 
            strm.VOIDHANDLERKEY, 
            strm.WEBFORMCONFIGKEY, 
            strm.WEBFORMHANDLERKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MERCHANTACCOUNTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_MERCHANTACCOUNT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MERCHANTACCOUNTKEY=src.MERCHANTACCOUNTKEY) OR (target.MERCHANTACCOUNTKEY IS NULL AND src.MERCHANTACCOUNTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTID=src.ACCOUNTID, 
                    target.ACCOUNTNAME=src.ACCOUNTNAME, 
                    target.ACCOUNTPASSWORD=src.ACCOUNTPASSWORD, 
                    target.ACCOUNTUSERNAME=src.ACCOUNTUSERNAME, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTHHANDLERKEY=src.AUTHHANDLERKEY, 
                    target.AUTHORIZECONFIGKEY=src.AUTHORIZECONFIGKEY, 
                    target.AUTHORIZEPAYMENTFORMULAKEY=src.AUTHORIZEPAYMENTFORMULAKEY, 
                    target.CARDNUMBERFORMAT=src.CARDNUMBERFORMAT, 
                    target.CARDNUMBERFORMATOVR=src.CARDNUMBERFORMATOVR, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.EFFECTIVEDATE=src.EFFECTIVEDATE, 
                    target.ENCRYPTCUSTPROP=src.ENCRYPTCUSTPROP, 
                    target.EXPIREDATE=src.EXPIREDATE, 
                    target.HOSTADDR=src.HOSTADDR, 
                    target.HOSTPORT=src.HOSTPORT, 
                    target.INQCONFIGKEY=src.INQCONFIGKEY, 
                    target.INQHANDLERKEY=src.INQHANDLERKEY, 
                    target.INQREVERSEDCONFIGKEY=src.INQREVERSEDCONFIGKEY, 
                    target.INQREVERSEDHANDLERKEY=src.INQREVERSEDHANDLERKEY, 
                    target.ISCHECKING=src.ISCHECKING, 
                    target.ISCREDIT=src.ISCREDIT, 
                    target.ISDEBIT=src.ISDEBIT, 
                    target.ISECOMMERCE=src.ISECOMMERCE, 
                    target.MERCHANTACCOUNTKEY=src.MERCHANTACCOUNTKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYMENTGATEWAYPROVIDER=src.PAYMENTGATEWAYPROVIDER, 
                    target.PAYMENTPROCESSING=src.PAYMENTPROCESSING, 
                    target.PREAUTHHANDLERKEY=src.PREAUTHHANDLERKEY, 
                    target.PREAUTHORIZECONFIGKEY=src.PREAUTHORIZECONFIGKEY, 
                    target.PREPCONFIGKEY=src.PREPCONFIGKEY, 
                    target.PREPHANDLERKEY=src.PREPHANDLERKEY, 
                    target.REDIRECTURLCONFIGKEY=src.REDIRECTURLCONFIGKEY, 
                    target.REDIRECTURLHANDLERKEY=src.REDIRECTURLHANDLERKEY, 
                    target.REVERSECONFIGKEY=src.REVERSECONFIGKEY, 
                    target.REVERSEHANDLERKEY=src.REVERSEHANDLERKEY, 
                    target.REVERSEPAYMENTFORMULAKEY=src.REVERSEPAYMENTFORMULAKEY, 
                    target.TIMEOUT=src.TIMEOUT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VOIDAPPROVEDTRANSACTIONONLY=src.VOIDAPPROVEDTRANSACTIONONLY, 
                    target.VOIDCONFIGKEY=src.VOIDCONFIGKEY, 
                    target.VOIDHANDLERKEY=src.VOIDHANDLERKEY, 
                    target.WEBFORMCONFIGKEY=src.WEBFORMCONFIGKEY, 
                    target.WEBFORMHANDLERKEY=src.WEBFORMHANDLERKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTID, 
                    ACCOUNTNAME, 
                    ACCOUNTPASSWORD, 
                    ACCOUNTUSERNAME, 
                    ADDBY, 
                    ADDDTTM, 
                    AUTHHANDLERKEY, 
                    AUTHORIZECONFIGKEY, 
                    AUTHORIZEPAYMENTFORMULAKEY, 
                    CARDNUMBERFORMAT, 
                    CARDNUMBERFORMATOVR, 
                    DATALAKE_DELETED, 
                    DESCRIPTION, 
                    EFFECTIVEDATE, 
                    ENCRYPTCUSTPROP, 
                    EXPIREDATE, 
                    HOSTADDR, 
                    HOSTPORT, 
                    INQCONFIGKEY, 
                    INQHANDLERKEY, 
                    INQREVERSEDCONFIGKEY, 
                    INQREVERSEDHANDLERKEY, 
                    ISCHECKING, 
                    ISCREDIT, 
                    ISDEBIT, 
                    ISECOMMERCE, 
                    MERCHANTACCOUNTKEY, 
                    MODBY, 
                    MODDTTM, 
                    PAYMENTGATEWAYPROVIDER, 
                    PAYMENTPROCESSING, 
                    PREAUTHHANDLERKEY, 
                    PREAUTHORIZECONFIGKEY, 
                    PREPCONFIGKEY, 
                    PREPHANDLERKEY, 
                    REDIRECTURLCONFIGKEY, 
                    REDIRECTURLHANDLERKEY, 
                    REVERSECONFIGKEY, 
                    REVERSEHANDLERKEY, 
                    REVERSEPAYMENTFORMULAKEY, 
                    TIMEOUT, 
                    VARIATION_ID, 
                    VOIDAPPROVEDTRANSACTIONONLY, 
                    VOIDCONFIGKEY, 
                    VOIDHANDLERKEY, 
                    WEBFORMCONFIGKEY, 
                    WEBFORMHANDLERKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTID, 
                    src.ACCOUNTNAME, 
                    src.ACCOUNTPASSWORD, 
                    src.ACCOUNTUSERNAME, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTHHANDLERKEY, 
                    src.AUTHORIZECONFIGKEY, 
                    src.AUTHORIZEPAYMENTFORMULAKEY, 
                    src.CARDNUMBERFORMAT, 
                    src.CARDNUMBERFORMATOVR, 
                    src.DATALAKE_DELETED, 
                    src.DESCRIPTION, 
                    src.EFFECTIVEDATE, 
                    src.ENCRYPTCUSTPROP, 
                    src.EXPIREDATE, 
                    src.HOSTADDR, 
                    src.HOSTPORT, 
                    src.INQCONFIGKEY, 
                    src.INQHANDLERKEY, 
                    src.INQREVERSEDCONFIGKEY, 
                    src.INQREVERSEDHANDLERKEY, 
                    src.ISCHECKING, 
                    src.ISCREDIT, 
                    src.ISDEBIT, 
                    src.ISECOMMERCE, 
                    src.MERCHANTACCOUNTKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYMENTGATEWAYPROVIDER, 
                    src.PAYMENTPROCESSING, 
                    src.PREAUTHHANDLERKEY, 
                    src.PREAUTHORIZECONFIGKEY, 
                    src.PREPCONFIGKEY, 
                    src.PREPHANDLERKEY, 
                    src.REDIRECTURLCONFIGKEY, 
                    src.REDIRECTURLHANDLERKEY, 
                    src.REVERSECONFIGKEY, 
                    src.REVERSEHANDLERKEY, 
                    src.REVERSEPAYMENTFORMULAKEY, 
                    src.TIMEOUT, 
                    src.VARIATION_ID, 
                    src.VOIDAPPROVEDTRANSACTIONONLY, 
                    src.VOIDCONFIGKEY, 
                    src.VOIDHANDLERKEY, 
                    src.WEBFORMCONFIGKEY, 
                    src.WEBFORMHANDLERKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_MERCHANTACCOUNT as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTID, 
            strm.ACCOUNTNAME, 
            strm.ACCOUNTPASSWORD, 
            strm.ACCOUNTUSERNAME, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTHHANDLERKEY, 
            strm.AUTHORIZECONFIGKEY, 
            strm.AUTHORIZEPAYMENTFORMULAKEY, 
            strm.CARDNUMBERFORMAT, 
            strm.CARDNUMBERFORMATOVR, 
            strm.DATALAKE_DELETED, 
            strm.DESCRIPTION, 
            strm.EFFECTIVEDATE, 
            strm.ENCRYPTCUSTPROP, 
            strm.EXPIREDATE, 
            strm.HOSTADDR, 
            strm.HOSTPORT, 
            strm.INQCONFIGKEY, 
            strm.INQHANDLERKEY, 
            strm.INQREVERSEDCONFIGKEY, 
            strm.INQREVERSEDHANDLERKEY, 
            strm.ISCHECKING, 
            strm.ISCREDIT, 
            strm.ISDEBIT, 
            strm.ISECOMMERCE, 
            strm.MERCHANTACCOUNTKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYMENTGATEWAYPROVIDER, 
            strm.PAYMENTPROCESSING, 
            strm.PREAUTHHANDLERKEY, 
            strm.PREAUTHORIZECONFIGKEY, 
            strm.PREPCONFIGKEY, 
            strm.PREPHANDLERKEY, 
            strm.REDIRECTURLCONFIGKEY, 
            strm.REDIRECTURLHANDLERKEY, 
            strm.REVERSECONFIGKEY, 
            strm.REVERSEHANDLERKEY, 
            strm.REVERSEPAYMENTFORMULAKEY, 
            strm.TIMEOUT, 
            strm.VARIATION_ID, 
            strm.VOIDAPPROVEDTRANSACTIONONLY, 
            strm.VOIDCONFIGKEY, 
            strm.VOIDHANDLERKEY, 
            strm.WEBFORMCONFIGKEY, 
            strm.WEBFORMHANDLERKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MERCHANTACCOUNTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_MERCHANTACCOUNT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MERCHANTACCOUNTKEY=src.MERCHANTACCOUNTKEY) OR (target.MERCHANTACCOUNTKEY IS NULL AND src.MERCHANTACCOUNTKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTID=src.ACCOUNTID, 
                    target.ACCOUNTNAME=src.ACCOUNTNAME, 
                    target.ACCOUNTPASSWORD=src.ACCOUNTPASSWORD, 
                    target.ACCOUNTUSERNAME=src.ACCOUNTUSERNAME, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTHHANDLERKEY=src.AUTHHANDLERKEY, 
                    target.AUTHORIZECONFIGKEY=src.AUTHORIZECONFIGKEY, 
                    target.AUTHORIZEPAYMENTFORMULAKEY=src.AUTHORIZEPAYMENTFORMULAKEY, 
                    target.CARDNUMBERFORMAT=src.CARDNUMBERFORMAT, 
                    target.CARDNUMBERFORMATOVR=src.CARDNUMBERFORMATOVR, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.EFFECTIVEDATE=src.EFFECTIVEDATE, 
                    target.ENCRYPTCUSTPROP=src.ENCRYPTCUSTPROP, 
                    target.EXPIREDATE=src.EXPIREDATE, 
                    target.HOSTADDR=src.HOSTADDR, 
                    target.HOSTPORT=src.HOSTPORT, 
                    target.INQCONFIGKEY=src.INQCONFIGKEY, 
                    target.INQHANDLERKEY=src.INQHANDLERKEY, 
                    target.INQREVERSEDCONFIGKEY=src.INQREVERSEDCONFIGKEY, 
                    target.INQREVERSEDHANDLERKEY=src.INQREVERSEDHANDLERKEY, 
                    target.ISCHECKING=src.ISCHECKING, 
                    target.ISCREDIT=src.ISCREDIT, 
                    target.ISDEBIT=src.ISDEBIT, 
                    target.ISECOMMERCE=src.ISECOMMERCE, 
                    target.MERCHANTACCOUNTKEY=src.MERCHANTACCOUNTKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYMENTGATEWAYPROVIDER=src.PAYMENTGATEWAYPROVIDER, 
                    target.PAYMENTPROCESSING=src.PAYMENTPROCESSING, 
                    target.PREAUTHHANDLERKEY=src.PREAUTHHANDLERKEY, 
                    target.PREAUTHORIZECONFIGKEY=src.PREAUTHORIZECONFIGKEY, 
                    target.PREPCONFIGKEY=src.PREPCONFIGKEY, 
                    target.PREPHANDLERKEY=src.PREPHANDLERKEY, 
                    target.REDIRECTURLCONFIGKEY=src.REDIRECTURLCONFIGKEY, 
                    target.REDIRECTURLHANDLERKEY=src.REDIRECTURLHANDLERKEY, 
                    target.REVERSECONFIGKEY=src.REVERSECONFIGKEY, 
                    target.REVERSEHANDLERKEY=src.REVERSEHANDLERKEY, 
                    target.REVERSEPAYMENTFORMULAKEY=src.REVERSEPAYMENTFORMULAKEY, 
                    target.TIMEOUT=src.TIMEOUT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VOIDAPPROVEDTRANSACTIONONLY=src.VOIDAPPROVEDTRANSACTIONONLY, 
                    target.VOIDCONFIGKEY=src.VOIDCONFIGKEY, 
                    target.VOIDHANDLERKEY=src.VOIDHANDLERKEY, 
                    target.WEBFORMCONFIGKEY=src.WEBFORMCONFIGKEY, 
                    target.WEBFORMHANDLERKEY=src.WEBFORMHANDLERKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTID, ACCOUNTNAME, ACCOUNTPASSWORD, ACCOUNTUSERNAME, ADDBY, ADDDTTM, AUTHHANDLERKEY, AUTHORIZECONFIGKEY, AUTHORIZEPAYMENTFORMULAKEY, CARDNUMBERFORMAT, CARDNUMBERFORMATOVR, DATALAKE_DELETED, DESCRIPTION, EFFECTIVEDATE, ENCRYPTCUSTPROP, EXPIREDATE, HOSTADDR, HOSTPORT, INQCONFIGKEY, INQHANDLERKEY, INQREVERSEDCONFIGKEY, INQREVERSEDHANDLERKEY, ISCHECKING, ISCREDIT, ISDEBIT, ISECOMMERCE, MERCHANTACCOUNTKEY, MODBY, MODDTTM, PAYMENTGATEWAYPROVIDER, PAYMENTPROCESSING, PREAUTHHANDLERKEY, PREAUTHORIZECONFIGKEY, PREPCONFIGKEY, PREPHANDLERKEY, REDIRECTURLCONFIGKEY, REDIRECTURLHANDLERKEY, REVERSECONFIGKEY, REVERSEHANDLERKEY, REVERSEPAYMENTFORMULAKEY, TIMEOUT, VARIATION_ID, VOIDAPPROVEDTRANSACTIONONLY, VOIDCONFIGKEY, VOIDHANDLERKEY, WEBFORMCONFIGKEY, WEBFORMHANDLERKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTID, 
                    src.ACCOUNTNAME, 
                    src.ACCOUNTPASSWORD, 
                    src.ACCOUNTUSERNAME, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTHHANDLERKEY, 
                    src.AUTHORIZECONFIGKEY, 
                    src.AUTHORIZEPAYMENTFORMULAKEY, 
                    src.CARDNUMBERFORMAT, 
                    src.CARDNUMBERFORMATOVR, 
                    src.DATALAKE_DELETED, 
                    src.DESCRIPTION, 
                    src.EFFECTIVEDATE, 
                    src.ENCRYPTCUSTPROP, 
                    src.EXPIREDATE, 
                    src.HOSTADDR, 
                    src.HOSTPORT, 
                    src.INQCONFIGKEY, 
                    src.INQHANDLERKEY, 
                    src.INQREVERSEDCONFIGKEY, 
                    src.INQREVERSEDHANDLERKEY, 
                    src.ISCHECKING, 
                    src.ISCREDIT, 
                    src.ISDEBIT, 
                    src.ISECOMMERCE, 
                    src.MERCHANTACCOUNTKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYMENTGATEWAYPROVIDER, 
                    src.PAYMENTPROCESSING, 
                    src.PREAUTHHANDLERKEY, 
                    src.PREAUTHORIZECONFIGKEY, 
                    src.PREPCONFIGKEY, 
                    src.PREPHANDLERKEY, 
                    src.REDIRECTURLCONFIGKEY, 
                    src.REDIRECTURLHANDLERKEY, 
                    src.REVERSECONFIGKEY, 
                    src.REVERSEHANDLERKEY, 
                    src.REVERSEPAYMENTFORMULAKEY, 
                    src.TIMEOUT, 
                    src.VARIATION_ID, 
                    src.VOIDAPPROVEDTRANSACTIONONLY, 
                    src.VOIDCONFIGKEY, 
                    src.VOIDHANDLERKEY, 
                    src.WEBFORMCONFIGKEY, 
                    src.WEBFORMHANDLERKEY,     
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