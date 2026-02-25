create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_ACCTCNTC()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCTCNTC_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_ACCTCNTC as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTCONTACTKEY, 
            strm.ACCOUNTKEY, 
            strm.ACTIVEFLAG, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CAPACITY, 
            strm.CAPFRDTTM, 
            strm.CAPTODTTM, 
            strm.CNTCTKEY, 
            strm.DELETED, 
            strm.ENTEFFDATE, 
            strm.ENTEXPDATE, 
            strm.ENTORD, 
            strm.ENTPCT, 
            strm.ENTVAL1, 
            strm.ENTVAL2, 
            strm.ENTVAL3, 
            strm.ENTVAL4, 
            strm.ENTVAL5, 
            strm.ENTVAL6, 
            strm.ENTVAL7, 
            strm.ENTVAL8, 
            strm.IDKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCOUNTCONTACTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCTCNTC as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCOUNTCONTACTKEY=src.ACCOUNTCONTACTKEY) OR (target.ACCOUNTCONTACTKEY IS NULL AND src.ACCOUNTCONTACTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTCONTACTKEY=src.ACCOUNTCONTACTKEY, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACTIVEFLAG=src.ACTIVEFLAG, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CAPACITY=src.CAPACITY, 
                    target.CAPFRDTTM=src.CAPFRDTTM, 
                    target.CAPTODTTM=src.CAPTODTTM, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.DELETED=src.DELETED, 
                    target.ENTEFFDATE=src.ENTEFFDATE, 
                    target.ENTEXPDATE=src.ENTEXPDATE, 
                    target.ENTORD=src.ENTORD, 
                    target.ENTPCT=src.ENTPCT, 
                    target.ENTVAL1=src.ENTVAL1, 
                    target.ENTVAL2=src.ENTVAL2, 
                    target.ENTVAL3=src.ENTVAL3, 
                    target.ENTVAL4=src.ENTVAL4, 
                    target.ENTVAL5=src.ENTVAL5, 
                    target.ENTVAL6=src.ENTVAL6, 
                    target.ENTVAL7=src.ENTVAL7, 
                    target.ENTVAL8=src.ENTVAL8, 
                    target.IDKEY=src.IDKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTCONTACTKEY, 
                    ACCOUNTKEY, 
                    ACTIVEFLAG, 
                    ADDBY, 
                    ADDDTTM, 
                    CAPACITY, 
                    CAPFRDTTM, 
                    CAPTODTTM, 
                    CNTCTKEY, 
                    DELETED, 
                    ENTEFFDATE, 
                    ENTEXPDATE, 
                    ENTORD, 
                    ENTPCT, 
                    ENTVAL1, 
                    ENTVAL2, 
                    ENTVAL3, 
                    ENTVAL4, 
                    ENTVAL5, 
                    ENTVAL6, 
                    ENTVAL7, 
                    ENTVAL8, 
                    IDKEY, 
                    MODBY, 
                    MODDTTM, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTCONTACTKEY, 
                    src.ACCOUNTKEY, 
                    src.ACTIVEFLAG, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CAPACITY, 
                    src.CAPFRDTTM, 
                    src.CAPTODTTM, 
                    src.CNTCTKEY, 
                    src.DELETED, 
                    src.ENTEFFDATE, 
                    src.ENTEXPDATE, 
                    src.ENTORD, 
                    src.ENTPCT, 
                    src.ENTVAL1, 
                    src.ENTVAL2, 
                    src.ENTVAL3, 
                    src.ENTVAL4, 
                    src.ENTVAL5, 
                    src.ENTVAL6, 
                    src.ENTVAL7, 
                    src.ENTVAL8, 
                    src.IDKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_ACCTCNTC as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTCONTACTKEY, 
            strm.ACCOUNTKEY, 
            strm.ACTIVEFLAG, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CAPACITY, 
            strm.CAPFRDTTM, 
            strm.CAPTODTTM, 
            strm.CNTCTKEY, 
            strm.DELETED, 
            strm.ENTEFFDATE, 
            strm.ENTEXPDATE, 
            strm.ENTORD, 
            strm.ENTPCT, 
            strm.ENTVAL1, 
            strm.ENTVAL2, 
            strm.ENTVAL3, 
            strm.ENTVAL4, 
            strm.ENTVAL5, 
            strm.ENTVAL6, 
            strm.ENTVAL7, 
            strm.ENTVAL8, 
            strm.IDKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCOUNTCONTACTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCTCNTC as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCOUNTCONTACTKEY=src.ACCOUNTCONTACTKEY) OR (target.ACCOUNTCONTACTKEY IS NULL AND src.ACCOUNTCONTACTKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTCONTACTKEY=src.ACCOUNTCONTACTKEY, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACTIVEFLAG=src.ACTIVEFLAG, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CAPACITY=src.CAPACITY, 
                    target.CAPFRDTTM=src.CAPFRDTTM, 
                    target.CAPTODTTM=src.CAPTODTTM, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.DELETED=src.DELETED, 
                    target.ENTEFFDATE=src.ENTEFFDATE, 
                    target.ENTEXPDATE=src.ENTEXPDATE, 
                    target.ENTORD=src.ENTORD, 
                    target.ENTPCT=src.ENTPCT, 
                    target.ENTVAL1=src.ENTVAL1, 
                    target.ENTVAL2=src.ENTVAL2, 
                    target.ENTVAL3=src.ENTVAL3, 
                    target.ENTVAL4=src.ENTVAL4, 
                    target.ENTVAL5=src.ENTVAL5, 
                    target.ENTVAL6=src.ENTVAL6, 
                    target.ENTVAL7=src.ENTVAL7, 
                    target.ENTVAL8=src.ENTVAL8, 
                    target.IDKEY=src.IDKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTCONTACTKEY, ACCOUNTKEY, ACTIVEFLAG, ADDBY, ADDDTTM, CAPACITY, CAPFRDTTM, CAPTODTTM, CNTCTKEY, DELETED, ENTEFFDATE, ENTEXPDATE, ENTORD, ENTPCT, ENTVAL1, ENTVAL2, ENTVAL3, ENTVAL4, ENTVAL5, ENTVAL6, ENTVAL7, ENTVAL8, IDKEY, MODBY, MODDTTM, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTCONTACTKEY, 
                    src.ACCOUNTKEY, 
                    src.ACTIVEFLAG, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CAPACITY, 
                    src.CAPFRDTTM, 
                    src.CAPTODTTM, 
                    src.CNTCTKEY, 
                    src.DELETED, 
                    src.ENTEFFDATE, 
                    src.ENTEXPDATE, 
                    src.ENTORD, 
                    src.ENTPCT, 
                    src.ENTVAL1, 
                    src.ENTVAL2, 
                    src.ENTVAL3, 
                    src.ENTVAL4, 
                    src.ENTVAL5, 
                    src.ENTVAL6, 
                    src.ENTVAL7, 
                    src.ENTVAL8, 
                    src.IDKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
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