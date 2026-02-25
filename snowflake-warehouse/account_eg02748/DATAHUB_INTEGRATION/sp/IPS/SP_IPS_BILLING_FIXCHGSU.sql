create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_FIXCHGSU()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_FIXCHGSU_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_FIXCHGSU as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLEDTHROUGHDATE1, 
            strm.BILLEDTHROUGHDATE10, 
            strm.BILLEDTHROUGHDATE11, 
            strm.BILLEDTHROUGHDATE12, 
            strm.BILLEDTHROUGHDATE2, 
            strm.BILLEDTHROUGHDATE3, 
            strm.BILLEDTHROUGHDATE4, 
            strm.BILLEDTHROUGHDATE5, 
            strm.BILLEDTHROUGHDATE6, 
            strm.BILLEDTHROUGHDATE7, 
            strm.BILLEDTHROUGHDATE8, 
            strm.BILLEDTHROUGHDATE9, 
            strm.DELETED, 
            strm.FIXCHGSUKEY, 
            strm.FREQUENCY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FIXCHGSUKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_FIXCHGSU as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FIXCHGSUKEY=src.FIXCHGSUKEY) OR (target.FIXCHGSUKEY IS NULL AND src.FIXCHGSUKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLEDTHROUGHDATE1=src.BILLEDTHROUGHDATE1, 
                    target.BILLEDTHROUGHDATE10=src.BILLEDTHROUGHDATE10, 
                    target.BILLEDTHROUGHDATE11=src.BILLEDTHROUGHDATE11, 
                    target.BILLEDTHROUGHDATE12=src.BILLEDTHROUGHDATE12, 
                    target.BILLEDTHROUGHDATE2=src.BILLEDTHROUGHDATE2, 
                    target.BILLEDTHROUGHDATE3=src.BILLEDTHROUGHDATE3, 
                    target.BILLEDTHROUGHDATE4=src.BILLEDTHROUGHDATE4, 
                    target.BILLEDTHROUGHDATE5=src.BILLEDTHROUGHDATE5, 
                    target.BILLEDTHROUGHDATE6=src.BILLEDTHROUGHDATE6, 
                    target.BILLEDTHROUGHDATE7=src.BILLEDTHROUGHDATE7, 
                    target.BILLEDTHROUGHDATE8=src.BILLEDTHROUGHDATE8, 
                    target.BILLEDTHROUGHDATE9=src.BILLEDTHROUGHDATE9, 
                    target.DELETED=src.DELETED, 
                    target.FIXCHGSUKEY=src.FIXCHGSUKEY, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    BILLEDTHROUGHDATE1, 
                    BILLEDTHROUGHDATE10, 
                    BILLEDTHROUGHDATE11, 
                    BILLEDTHROUGHDATE12, 
                    BILLEDTHROUGHDATE2, 
                    BILLEDTHROUGHDATE3, 
                    BILLEDTHROUGHDATE4, 
                    BILLEDTHROUGHDATE5, 
                    BILLEDTHROUGHDATE6, 
                    BILLEDTHROUGHDATE7, 
                    BILLEDTHROUGHDATE8, 
                    BILLEDTHROUGHDATE9, 
                    DELETED, 
                    FIXCHGSUKEY, 
                    FREQUENCY, 
                    MODBY, 
                    MODDTTM, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLEDTHROUGHDATE1, 
                    src.BILLEDTHROUGHDATE10, 
                    src.BILLEDTHROUGHDATE11, 
                    src.BILLEDTHROUGHDATE12, 
                    src.BILLEDTHROUGHDATE2, 
                    src.BILLEDTHROUGHDATE3, 
                    src.BILLEDTHROUGHDATE4, 
                    src.BILLEDTHROUGHDATE5, 
                    src.BILLEDTHROUGHDATE6, 
                    src.BILLEDTHROUGHDATE7, 
                    src.BILLEDTHROUGHDATE8, 
                    src.BILLEDTHROUGHDATE9, 
                    src.DELETED, 
                    src.FIXCHGSUKEY, 
                    src.FREQUENCY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_FIXCHGSU as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLEDTHROUGHDATE1, 
            strm.BILLEDTHROUGHDATE10, 
            strm.BILLEDTHROUGHDATE11, 
            strm.BILLEDTHROUGHDATE12, 
            strm.BILLEDTHROUGHDATE2, 
            strm.BILLEDTHROUGHDATE3, 
            strm.BILLEDTHROUGHDATE4, 
            strm.BILLEDTHROUGHDATE5, 
            strm.BILLEDTHROUGHDATE6, 
            strm.BILLEDTHROUGHDATE7, 
            strm.BILLEDTHROUGHDATE8, 
            strm.BILLEDTHROUGHDATE9, 
            strm.DELETED, 
            strm.FIXCHGSUKEY, 
            strm.FREQUENCY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FIXCHGSUKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_FIXCHGSU as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FIXCHGSUKEY=src.FIXCHGSUKEY) OR (target.FIXCHGSUKEY IS NULL AND src.FIXCHGSUKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLEDTHROUGHDATE1=src.BILLEDTHROUGHDATE1, 
                    target.BILLEDTHROUGHDATE10=src.BILLEDTHROUGHDATE10, 
                    target.BILLEDTHROUGHDATE11=src.BILLEDTHROUGHDATE11, 
                    target.BILLEDTHROUGHDATE12=src.BILLEDTHROUGHDATE12, 
                    target.BILLEDTHROUGHDATE2=src.BILLEDTHROUGHDATE2, 
                    target.BILLEDTHROUGHDATE3=src.BILLEDTHROUGHDATE3, 
                    target.BILLEDTHROUGHDATE4=src.BILLEDTHROUGHDATE4, 
                    target.BILLEDTHROUGHDATE5=src.BILLEDTHROUGHDATE5, 
                    target.BILLEDTHROUGHDATE6=src.BILLEDTHROUGHDATE6, 
                    target.BILLEDTHROUGHDATE7=src.BILLEDTHROUGHDATE7, 
                    target.BILLEDTHROUGHDATE8=src.BILLEDTHROUGHDATE8, 
                    target.BILLEDTHROUGHDATE9=src.BILLEDTHROUGHDATE9, 
                    target.DELETED=src.DELETED, 
                    target.FIXCHGSUKEY=src.FIXCHGSUKEY, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, BILLEDTHROUGHDATE1, BILLEDTHROUGHDATE10, BILLEDTHROUGHDATE11, BILLEDTHROUGHDATE12, BILLEDTHROUGHDATE2, BILLEDTHROUGHDATE3, BILLEDTHROUGHDATE4, BILLEDTHROUGHDATE5, BILLEDTHROUGHDATE6, BILLEDTHROUGHDATE7, BILLEDTHROUGHDATE8, BILLEDTHROUGHDATE9, DELETED, FIXCHGSUKEY, FREQUENCY, MODBY, MODDTTM, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLEDTHROUGHDATE1, 
                    src.BILLEDTHROUGHDATE10, 
                    src.BILLEDTHROUGHDATE11, 
                    src.BILLEDTHROUGHDATE12, 
                    src.BILLEDTHROUGHDATE2, 
                    src.BILLEDTHROUGHDATE3, 
                    src.BILLEDTHROUGHDATE4, 
                    src.BILLEDTHROUGHDATE5, 
                    src.BILLEDTHROUGHDATE6, 
                    src.BILLEDTHROUGHDATE7, 
                    src.BILLEDTHROUGHDATE8, 
                    src.BILLEDTHROUGHDATE9, 
                    src.DELETED, 
                    src.FIXCHGSUKEY, 
                    src.FREQUENCY, 
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