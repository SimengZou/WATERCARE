create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_BUILDING_BLDGPROCSTATECHECK()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGPROCSTATECHECK_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGPROCSTATECHECK as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGPROCESSSTATEKEY, 
            strm.APBLDGPROCSTATECHKKEY, 
            strm.CHECKFRMLA, 
            strm.CHECKTITLE, 
            strm.CHECKTYPE, 
            strm.DATALAKE_DELETED, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXCLUDEFROMPENDINGPORTAL, 
            strm.EXPDATE, 
            strm.INTERNALONLYFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PORTALDESCRIPTION, 
            strm.SHOWINPORTAL, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APBLDGPROCSTATECHKKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_BUILDING_BLDGPROCSTATECHECK as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APBLDGPROCSTATECHKKEY=src.APBLDGPROCSTATECHKKEY) OR (target.APBLDGPROCSTATECHKKEY IS NULL AND src.APBLDGPROCSTATECHKKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGPROCESSSTATEKEY=src.APBLDGPROCESSSTATEKEY, 
                    target.APBLDGPROCSTATECHKKEY=src.APBLDGPROCSTATECHKKEY, 
                    target.CHECKFRMLA=src.CHECKFRMLA, 
                    target.CHECKTITLE=src.CHECKTITLE, 
                    target.CHECKTYPE=src.CHECKTYPE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXCLUDEFROMPENDINGPORTAL=src.EXCLUDEFROMPENDINGPORTAL, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTERNALONLYFLAG=src.INTERNALONLYFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.SHOWINPORTAL=src.SHOWINPORTAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APBLDGPROCESSSTATEKEY, 
                    APBLDGPROCSTATECHKKEY, 
                    CHECKFRMLA, 
                    CHECKTITLE, 
                    CHECKTYPE, 
                    DATALAKE_DELETED, 
                    DPDESC, 
                    EFFDATE, 
                    EXCLUDEFROMPENDINGPORTAL, 
                    EXPDATE, 
                    INTERNALONLYFLAG, 
                    MODBY, 
                    MODDTTM, 
                    PORTALDESCRIPTION, 
                    SHOWINPORTAL, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGPROCESSSTATEKEY, 
                    src.APBLDGPROCSTATECHKKEY, 
                    src.CHECKFRMLA, 
                    src.CHECKTITLE, 
                    src.CHECKTYPE, 
                    src.DATALAKE_DELETED, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXCLUDEFROMPENDINGPORTAL, 
                    src.EXPDATE, 
                    src.INTERNALONLYFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PORTALDESCRIPTION, 
                    src.SHOWINPORTAL, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGPROCSTATECHECK as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGPROCESSSTATEKEY, 
            strm.APBLDGPROCSTATECHKKEY, 
            strm.CHECKFRMLA, 
            strm.CHECKTITLE, 
            strm.CHECKTYPE, 
            strm.DATALAKE_DELETED, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXCLUDEFROMPENDINGPORTAL, 
            strm.EXPDATE, 
            strm.INTERNALONLYFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PORTALDESCRIPTION, 
            strm.SHOWINPORTAL, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APBLDGPROCSTATECHKKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_BUILDING_BLDGPROCSTATECHECK as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APBLDGPROCSTATECHKKEY=src.APBLDGPROCSTATECHKKEY) OR (target.APBLDGPROCSTATECHKKEY IS NULL AND src.APBLDGPROCSTATECHKKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGPROCESSSTATEKEY=src.APBLDGPROCESSSTATEKEY, 
                    target.APBLDGPROCSTATECHKKEY=src.APBLDGPROCSTATECHKKEY, 
                    target.CHECKFRMLA=src.CHECKFRMLA, 
                    target.CHECKTITLE=src.CHECKTITLE, 
                    target.CHECKTYPE=src.CHECKTYPE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXCLUDEFROMPENDINGPORTAL=src.EXCLUDEFROMPENDINGPORTAL, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTERNALONLYFLAG=src.INTERNALONLYFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.SHOWINPORTAL=src.SHOWINPORTAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APBLDGPROCESSSTATEKEY, APBLDGPROCSTATECHKKEY, CHECKFRMLA, CHECKTITLE, CHECKTYPE, DATALAKE_DELETED, DPDESC, EFFDATE, EXCLUDEFROMPENDINGPORTAL, EXPDATE, INTERNALONLYFLAG, MODBY, MODDTTM, PORTALDESCRIPTION, SHOWINPORTAL, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGPROCESSSTATEKEY, 
                    src.APBLDGPROCSTATECHKKEY, 
                    src.CHECKFRMLA, 
                    src.CHECKTITLE, 
                    src.CHECKTYPE, 
                    src.DATALAKE_DELETED, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXCLUDEFROMPENDINGPORTAL, 
                    src.EXPDATE, 
                    src.INTERNALONLYFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PORTALDESCRIPTION, 
                    src.SHOWINPORTAL, 
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