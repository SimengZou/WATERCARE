create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CASHIERING_CASHIERINGSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CASHIERING_CASHIERINGSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CASHIERING_CASHIERINGSETUP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILL1, 
            strm.BILL2, 
            strm.BILL3, 
            strm.BILL4, 
            strm.BILL5, 
            strm.BILL6, 
            strm.BILL7, 
            strm.BILL8, 
            strm.CASHBGTNO, 
            strm.CASHSUKEY, 
            strm.COIN1, 
            strm.COIN2, 
            strm.COIN3, 
            strm.COIN4, 
            strm.COIN5, 
            strm.COIN6, 
            strm.COIN7, 
            strm.COIN8, 
            strm.COINROLL1, 
            strm.COINROLL2, 
            strm.COINROLL3, 
            strm.COINROLL4, 
            strm.COINROLL5, 
            strm.COINROLL6, 
            strm.COINROLL7, 
            strm.COINROLL8, 
            strm.DATALAKE_DELETED, 
            strm.DRWRSTYLE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.RECEIPTFRMLAKEY, 
            strm.TAXRATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CASHSUKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_CASHIERINGSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CASHSUKEY=src.CASHSUKEY) OR (target.CASHSUKEY IS NULL AND src.CASHSUKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILL1=src.BILL1, 
                    target.BILL2=src.BILL2, 
                    target.BILL3=src.BILL3, 
                    target.BILL4=src.BILL4, 
                    target.BILL5=src.BILL5, 
                    target.BILL6=src.BILL6, 
                    target.BILL7=src.BILL7, 
                    target.BILL8=src.BILL8, 
                    target.CASHBGTNO=src.CASHBGTNO, 
                    target.CASHSUKEY=src.CASHSUKEY, 
                    target.COIN1=src.COIN1, 
                    target.COIN2=src.COIN2, 
                    target.COIN3=src.COIN3, 
                    target.COIN4=src.COIN4, 
                    target.COIN5=src.COIN5, 
                    target.COIN6=src.COIN6, 
                    target.COIN7=src.COIN7, 
                    target.COIN8=src.COIN8, 
                    target.COINROLL1=src.COINROLL1, 
                    target.COINROLL2=src.COINROLL2, 
                    target.COINROLL3=src.COINROLL3, 
                    target.COINROLL4=src.COINROLL4, 
                    target.COINROLL5=src.COINROLL5, 
                    target.COINROLL6=src.COINROLL6, 
                    target.COINROLL7=src.COINROLL7, 
                    target.COINROLL8=src.COINROLL8, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DRWRSTYLE=src.DRWRSTYLE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.RECEIPTFRMLAKEY=src.RECEIPTFRMLAKEY, 
                    target.TAXRATE=src.TAXRATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    BILL1, 
                    BILL2, 
                    BILL3, 
                    BILL4, 
                    BILL5, 
                    BILL6, 
                    BILL7, 
                    BILL8, 
                    CASHBGTNO, 
                    CASHSUKEY, 
                    COIN1, 
                    COIN2, 
                    COIN3, 
                    COIN4, 
                    COIN5, 
                    COIN6, 
                    COIN7, 
                    COIN8, 
                    COINROLL1, 
                    COINROLL2, 
                    COINROLL3, 
                    COINROLL4, 
                    COINROLL5, 
                    COINROLL6, 
                    COINROLL7, 
                    COINROLL8, 
                    DATALAKE_DELETED, 
                    DRWRSTYLE, 
                    MODBY, 
                    MODDTTM, 
                    RECEIPTFRMLAKEY, 
                    TAXRATE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILL1, 
                    src.BILL2, 
                    src.BILL3, 
                    src.BILL4, 
                    src.BILL5, 
                    src.BILL6, 
                    src.BILL7, 
                    src.BILL8, 
                    src.CASHBGTNO, 
                    src.CASHSUKEY, 
                    src.COIN1, 
                    src.COIN2, 
                    src.COIN3, 
                    src.COIN4, 
                    src.COIN5, 
                    src.COIN6, 
                    src.COIN7, 
                    src.COIN8, 
                    src.COINROLL1, 
                    src.COINROLL2, 
                    src.COINROLL3, 
                    src.COINROLL4, 
                    src.COINROLL5, 
                    src.COINROLL6, 
                    src.COINROLL7, 
                    src.COINROLL8, 
                    src.DATALAKE_DELETED, 
                    src.DRWRSTYLE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.RECEIPTFRMLAKEY, 
                    src.TAXRATE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_CASHIERINGSETUP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILL1, 
            strm.BILL2, 
            strm.BILL3, 
            strm.BILL4, 
            strm.BILL5, 
            strm.BILL6, 
            strm.BILL7, 
            strm.BILL8, 
            strm.CASHBGTNO, 
            strm.CASHSUKEY, 
            strm.COIN1, 
            strm.COIN2, 
            strm.COIN3, 
            strm.COIN4, 
            strm.COIN5, 
            strm.COIN6, 
            strm.COIN7, 
            strm.COIN8, 
            strm.COINROLL1, 
            strm.COINROLL2, 
            strm.COINROLL3, 
            strm.COINROLL4, 
            strm.COINROLL5, 
            strm.COINROLL6, 
            strm.COINROLL7, 
            strm.COINROLL8, 
            strm.DATALAKE_DELETED, 
            strm.DRWRSTYLE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.RECEIPTFRMLAKEY, 
            strm.TAXRATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CASHSUKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_CASHIERINGSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CASHSUKEY=src.CASHSUKEY) OR (target.CASHSUKEY IS NULL AND src.CASHSUKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILL1=src.BILL1, 
                    target.BILL2=src.BILL2, 
                    target.BILL3=src.BILL3, 
                    target.BILL4=src.BILL4, 
                    target.BILL5=src.BILL5, 
                    target.BILL6=src.BILL6, 
                    target.BILL7=src.BILL7, 
                    target.BILL8=src.BILL8, 
                    target.CASHBGTNO=src.CASHBGTNO, 
                    target.CASHSUKEY=src.CASHSUKEY, 
                    target.COIN1=src.COIN1, 
                    target.COIN2=src.COIN2, 
                    target.COIN3=src.COIN3, 
                    target.COIN4=src.COIN4, 
                    target.COIN5=src.COIN5, 
                    target.COIN6=src.COIN6, 
                    target.COIN7=src.COIN7, 
                    target.COIN8=src.COIN8, 
                    target.COINROLL1=src.COINROLL1, 
                    target.COINROLL2=src.COINROLL2, 
                    target.COINROLL3=src.COINROLL3, 
                    target.COINROLL4=src.COINROLL4, 
                    target.COINROLL5=src.COINROLL5, 
                    target.COINROLL6=src.COINROLL6, 
                    target.COINROLL7=src.COINROLL7, 
                    target.COINROLL8=src.COINROLL8, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DRWRSTYLE=src.DRWRSTYLE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.RECEIPTFRMLAKEY=src.RECEIPTFRMLAKEY, 
                    target.TAXRATE=src.TAXRATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, BILL1, BILL2, BILL3, BILL4, BILL5, BILL6, BILL7, BILL8, CASHBGTNO, CASHSUKEY, COIN1, COIN2, COIN3, COIN4, COIN5, COIN6, COIN7, COIN8, COINROLL1, COINROLL2, COINROLL3, COINROLL4, COINROLL5, COINROLL6, COINROLL7, COINROLL8, DATALAKE_DELETED, DRWRSTYLE, MODBY, MODDTTM, RECEIPTFRMLAKEY, TAXRATE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILL1, 
                    src.BILL2, 
                    src.BILL3, 
                    src.BILL4, 
                    src.BILL5, 
                    src.BILL6, 
                    src.BILL7, 
                    src.BILL8, 
                    src.CASHBGTNO, 
                    src.CASHSUKEY, 
                    src.COIN1, 
                    src.COIN2, 
                    src.COIN3, 
                    src.COIN4, 
                    src.COIN5, 
                    src.COIN6, 
                    src.COIN7, 
                    src.COIN8, 
                    src.COINROLL1, 
                    src.COINROLL2, 
                    src.COINROLL3, 
                    src.COINROLL4, 
                    src.COINROLL5, 
                    src.COINROLL6, 
                    src.COINROLL7, 
                    src.COINROLL8, 
                    src.DATALAKE_DELETED, 
                    src.DRWRSTYLE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.RECEIPTFRMLAKEY, 
                    src.TAXRATE, 
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