create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_PAYMENTARRANGMENT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYMENTARRANGMENT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_PAYMENTARRANGMENT as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOWWARNED, 
            strm.ARRANGEDAMOUNTLTD, 
            strm.ARRANGEDBY, 
            strm.ARRANGEDDTTM, 
            strm.ARRANGEDPAYMENTAMT, 
            strm.ARRANGEDTOTALAMT, 
            strm.ARRANGEDVSBILLEDPERCENT, 
            strm.ARRANGEMENTBALANCE, 
            strm.ARRANGEMENTCATEGORY, 
            strm.ARRANGEMENTSTATUS, 
            strm.ARRANGEMENTTEMPLATEKEY, 
            strm.ARRANGEMENTTYPE, 
            strm.BILLEDAMOUNTLTD, 
            strm.DELETED, 
            strm.DELINQUENTBALANCE, 
            strm.DOWNPAYMENTAMT, 
            strm.DOWNPAYMENTDUEDTTM, 
            strm.ENDDTTM, 
            strm.FREQUENCY, 
            strm.GUARANTORKEY, 
            strm.INCLUDECURRENTFLAG, 
            strm.INCLUDEDEPOSITFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFPAYMENTS, 
            strm.PARENTARRANGEMENTKEY, 
            strm.PAYMENTARRANGMENTKEY, 
            strm.PAYMENTSAVERAGEDFLAG, 
            strm.PAYMTSONLYEXCEPTFLAG, 
            strm.PAYOLDESTFIRSTFLAG, 
            strm.SENDNOTICESFLAG, 
            strm.STARTDTTM, 
            strm.STATUSDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PAYMENTARRANGMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYMENTARRANGMENT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PAYMENTARRANGMENTKEY=src.PAYMENTARRANGMENTKEY) OR (target.PAYMENTARRANGMENTKEY IS NULL AND src.PAYMENTARRANGMENTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOWWARNED=src.ALLOWWARNED, 
                    target.ARRANGEDAMOUNTLTD=src.ARRANGEDAMOUNTLTD, 
                    target.ARRANGEDBY=src.ARRANGEDBY, 
                    target.ARRANGEDDTTM=src.ARRANGEDDTTM, 
                    target.ARRANGEDPAYMENTAMT=src.ARRANGEDPAYMENTAMT, 
                    target.ARRANGEDTOTALAMT=src.ARRANGEDTOTALAMT, 
                    target.ARRANGEDVSBILLEDPERCENT=src.ARRANGEDVSBILLEDPERCENT, 
                    target.ARRANGEMENTBALANCE=src.ARRANGEMENTBALANCE, 
                    target.ARRANGEMENTCATEGORY=src.ARRANGEMENTCATEGORY, 
                    target.ARRANGEMENTSTATUS=src.ARRANGEMENTSTATUS, 
                    target.ARRANGEMENTTEMPLATEKEY=src.ARRANGEMENTTEMPLATEKEY, 
                    target.ARRANGEMENTTYPE=src.ARRANGEMENTTYPE, 
                    target.BILLEDAMOUNTLTD=src.BILLEDAMOUNTLTD, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENTBALANCE=src.DELINQUENTBALANCE, 
                    target.DOWNPAYMENTAMT=src.DOWNPAYMENTAMT, 
                    target.DOWNPAYMENTDUEDTTM=src.DOWNPAYMENTDUEDTTM, 
                    target.ENDDTTM=src.ENDDTTM, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.GUARANTORKEY=src.GUARANTORKEY, 
                    target.INCLUDECURRENTFLAG=src.INCLUDECURRENTFLAG, 
                    target.INCLUDEDEPOSITFLAG=src.INCLUDEDEPOSITFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFPAYMENTS=src.NUMBEROFPAYMENTS, 
                    target.PARENTARRANGEMENTKEY=src.PARENTARRANGEMENTKEY, 
                    target.PAYMENTARRANGMENTKEY=src.PAYMENTARRANGMENTKEY, 
                    target.PAYMENTSAVERAGEDFLAG=src.PAYMENTSAVERAGEDFLAG, 
                    target.PAYMTSONLYEXCEPTFLAG=src.PAYMTSONLYEXCEPTFLAG, 
                    target.PAYOLDESTFIRSTFLAG=src.PAYOLDESTFIRSTFLAG, 
                    target.SENDNOTICESFLAG=src.SENDNOTICESFLAG, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STATUSDTTM=src.STATUSDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    ALLOWWARNED, 
                    ARRANGEDAMOUNTLTD, 
                    ARRANGEDBY, 
                    ARRANGEDDTTM, 
                    ARRANGEDPAYMENTAMT, 
                    ARRANGEDTOTALAMT, 
                    ARRANGEDVSBILLEDPERCENT, 
                    ARRANGEMENTBALANCE, 
                    ARRANGEMENTCATEGORY, 
                    ARRANGEMENTSTATUS, 
                    ARRANGEMENTTEMPLATEKEY, 
                    ARRANGEMENTTYPE, 
                    BILLEDAMOUNTLTD, 
                    DELETED, 
                    DELINQUENTBALANCE, 
                    DOWNPAYMENTAMT, 
                    DOWNPAYMENTDUEDTTM, 
                    ENDDTTM, 
                    FREQUENCY, 
                    GUARANTORKEY, 
                    INCLUDECURRENTFLAG, 
                    INCLUDEDEPOSITFLAG, 
                    MODBY, 
                    MODDTTM, 
                    NUMBEROFPAYMENTS, 
                    PARENTARRANGEMENTKEY, 
                    PAYMENTARRANGMENTKEY, 
                    PAYMENTSAVERAGEDFLAG, 
                    PAYMTSONLYEXCEPTFLAG, 
                    PAYOLDESTFIRSTFLAG, 
                    SENDNOTICESFLAG, 
                    STARTDTTM, 
                    STATUSDTTM, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOWWARNED, 
                    src.ARRANGEDAMOUNTLTD, 
                    src.ARRANGEDBY, 
                    src.ARRANGEDDTTM, 
                    src.ARRANGEDPAYMENTAMT, 
                    src.ARRANGEDTOTALAMT, 
                    src.ARRANGEDVSBILLEDPERCENT, 
                    src.ARRANGEMENTBALANCE, 
                    src.ARRANGEMENTCATEGORY, 
                    src.ARRANGEMENTSTATUS, 
                    src.ARRANGEMENTTEMPLATEKEY, 
                    src.ARRANGEMENTTYPE, 
                    src.BILLEDAMOUNTLTD, 
                    src.DELETED, 
                    src.DELINQUENTBALANCE, 
                    src.DOWNPAYMENTAMT, 
                    src.DOWNPAYMENTDUEDTTM, 
                    src.ENDDTTM, 
                    src.FREQUENCY, 
                    src.GUARANTORKEY, 
                    src.INCLUDECURRENTFLAG, 
                    src.INCLUDEDEPOSITFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFPAYMENTS, 
                    src.PARENTARRANGEMENTKEY, 
                    src.PAYMENTARRANGMENTKEY, 
                    src.PAYMENTSAVERAGEDFLAG, 
                    src.PAYMTSONLYEXCEPTFLAG, 
                    src.PAYOLDESTFIRSTFLAG, 
                    src.SENDNOTICESFLAG, 
                    src.STARTDTTM, 
                    src.STATUSDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_PAYMENTARRANGMENT as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOWWARNED, 
            strm.ARRANGEDAMOUNTLTD, 
            strm.ARRANGEDBY, 
            strm.ARRANGEDDTTM, 
            strm.ARRANGEDPAYMENTAMT, 
            strm.ARRANGEDTOTALAMT, 
            strm.ARRANGEDVSBILLEDPERCENT, 
            strm.ARRANGEMENTBALANCE, 
            strm.ARRANGEMENTCATEGORY, 
            strm.ARRANGEMENTSTATUS, 
            strm.ARRANGEMENTTEMPLATEKEY, 
            strm.ARRANGEMENTTYPE, 
            strm.BILLEDAMOUNTLTD, 
            strm.DELETED, 
            strm.DELINQUENTBALANCE, 
            strm.DOWNPAYMENTAMT, 
            strm.DOWNPAYMENTDUEDTTM, 
            strm.ENDDTTM, 
            strm.FREQUENCY, 
            strm.GUARANTORKEY, 
            strm.INCLUDECURRENTFLAG, 
            strm.INCLUDEDEPOSITFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFPAYMENTS, 
            strm.PARENTARRANGEMENTKEY, 
            strm.PAYMENTARRANGMENTKEY, 
            strm.PAYMENTSAVERAGEDFLAG, 
            strm.PAYMTSONLYEXCEPTFLAG, 
            strm.PAYOLDESTFIRSTFLAG, 
            strm.SENDNOTICESFLAG, 
            strm.STARTDTTM, 
            strm.STATUSDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PAYMENTARRANGMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYMENTARRANGMENT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PAYMENTARRANGMENTKEY=src.PAYMENTARRANGMENTKEY) OR (target.PAYMENTARRANGMENTKEY IS NULL AND src.PAYMENTARRANGMENTKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOWWARNED=src.ALLOWWARNED, 
                    target.ARRANGEDAMOUNTLTD=src.ARRANGEDAMOUNTLTD, 
                    target.ARRANGEDBY=src.ARRANGEDBY, 
                    target.ARRANGEDDTTM=src.ARRANGEDDTTM, 
                    target.ARRANGEDPAYMENTAMT=src.ARRANGEDPAYMENTAMT, 
                    target.ARRANGEDTOTALAMT=src.ARRANGEDTOTALAMT, 
                    target.ARRANGEDVSBILLEDPERCENT=src.ARRANGEDVSBILLEDPERCENT, 
                    target.ARRANGEMENTBALANCE=src.ARRANGEMENTBALANCE, 
                    target.ARRANGEMENTCATEGORY=src.ARRANGEMENTCATEGORY, 
                    target.ARRANGEMENTSTATUS=src.ARRANGEMENTSTATUS, 
                    target.ARRANGEMENTTEMPLATEKEY=src.ARRANGEMENTTEMPLATEKEY, 
                    target.ARRANGEMENTTYPE=src.ARRANGEMENTTYPE, 
                    target.BILLEDAMOUNTLTD=src.BILLEDAMOUNTLTD, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENTBALANCE=src.DELINQUENTBALANCE, 
                    target.DOWNPAYMENTAMT=src.DOWNPAYMENTAMT, 
                    target.DOWNPAYMENTDUEDTTM=src.DOWNPAYMENTDUEDTTM, 
                    target.ENDDTTM=src.ENDDTTM, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.GUARANTORKEY=src.GUARANTORKEY, 
                    target.INCLUDECURRENTFLAG=src.INCLUDECURRENTFLAG, 
                    target.INCLUDEDEPOSITFLAG=src.INCLUDEDEPOSITFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFPAYMENTS=src.NUMBEROFPAYMENTS, 
                    target.PARENTARRANGEMENTKEY=src.PARENTARRANGEMENTKEY, 
                    target.PAYMENTARRANGMENTKEY=src.PAYMENTARRANGMENTKEY, 
                    target.PAYMENTSAVERAGEDFLAG=src.PAYMENTSAVERAGEDFLAG, 
                    target.PAYMTSONLYEXCEPTFLAG=src.PAYMTSONLYEXCEPTFLAG, 
                    target.PAYOLDESTFIRSTFLAG=src.PAYOLDESTFIRSTFLAG, 
                    target.SENDNOTICESFLAG=src.SENDNOTICESFLAG, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STATUSDTTM=src.STATUSDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ADDBY, ADDDTTM, ALLOWWARNED, ARRANGEDAMOUNTLTD, ARRANGEDBY, ARRANGEDDTTM, ARRANGEDPAYMENTAMT, ARRANGEDTOTALAMT, ARRANGEDVSBILLEDPERCENT, ARRANGEMENTBALANCE, ARRANGEMENTCATEGORY, ARRANGEMENTSTATUS, ARRANGEMENTTEMPLATEKEY, ARRANGEMENTTYPE, BILLEDAMOUNTLTD, DELETED, DELINQUENTBALANCE, DOWNPAYMENTAMT, DOWNPAYMENTDUEDTTM, ENDDTTM, FREQUENCY, GUARANTORKEY, INCLUDECURRENTFLAG, INCLUDEDEPOSITFLAG, MODBY, MODDTTM, NUMBEROFPAYMENTS, PARENTARRANGEMENTKEY, PAYMENTARRANGMENTKEY, PAYMENTSAVERAGEDFLAG, PAYMTSONLYEXCEPTFLAG, PAYOLDESTFIRSTFLAG, SENDNOTICESFLAG, STARTDTTM, STATUSDTTM, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOWWARNED, 
                    src.ARRANGEDAMOUNTLTD, 
                    src.ARRANGEDBY, 
                    src.ARRANGEDDTTM, 
                    src.ARRANGEDPAYMENTAMT, 
                    src.ARRANGEDTOTALAMT, 
                    src.ARRANGEDVSBILLEDPERCENT, 
                    src.ARRANGEMENTBALANCE, 
                    src.ARRANGEMENTCATEGORY, 
                    src.ARRANGEMENTSTATUS, 
                    src.ARRANGEMENTTEMPLATEKEY, 
                    src.ARRANGEMENTTYPE, 
                    src.BILLEDAMOUNTLTD, 
                    src.DELETED, 
                    src.DELINQUENTBALANCE, 
                    src.DOWNPAYMENTAMT, 
                    src.DOWNPAYMENTDUEDTTM, 
                    src.ENDDTTM, 
                    src.FREQUENCY, 
                    src.GUARANTORKEY, 
                    src.INCLUDECURRENTFLAG, 
                    src.INCLUDEDEPOSITFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFPAYMENTS, 
                    src.PARENTARRANGEMENTKEY, 
                    src.PAYMENTARRANGMENTKEY, 
                    src.PAYMENTSAVERAGEDFLAG, 
                    src.PAYMTSONLYEXCEPTFLAG, 
                    src.PAYOLDESTFIRSTFLAG, 
                    src.SENDNOTICESFLAG, 
                    src.STARTDTTM, 
                    src.STATUSDTTM, 
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