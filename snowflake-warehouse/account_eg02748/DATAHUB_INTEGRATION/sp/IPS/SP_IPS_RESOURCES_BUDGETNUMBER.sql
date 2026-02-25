create or replace procedure DATAHUB_INTEGRATION.SP_IPS_RESOURCES_BUDGETNUMBER()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_RESOURCES_BUDGETNUMBER_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_RESOURCES_BUDGETNUMBER as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BUDGETNUMBERKEY, 
            strm.CODE, 
            strm.DELETED, 
            strm.DESCRIPT, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.SEGMENT1, 
            strm.SEGMENT10, 
            strm.SEGMENT11, 
            strm.SEGMENT12, 
            strm.SEGMENT13, 
            strm.SEGMENT14, 
            strm.SEGMENT15, 
            strm.SEGMENT2, 
            strm.SEGMENT3, 
            strm.SEGMENT4, 
            strm.SEGMENT5, 
            strm.SEGMENT6, 
            strm.SEGMENT7, 
            strm.SEGMENT8, 
            strm.SEGMENT9, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BUDGETNUMBERKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_BUDGETNUMBER as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BUDGETNUMBERKEY=src.BUDGETNUMBERKEY) OR (target.BUDGETNUMBERKEY IS NULL AND src.BUDGETNUMBERKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BUDGETNUMBERKEY=src.BUDGETNUMBERKEY, 
                    target.CODE=src.CODE, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.SEGMENT1=src.SEGMENT1, 
                    target.SEGMENT10=src.SEGMENT10, 
                    target.SEGMENT11=src.SEGMENT11, 
                    target.SEGMENT12=src.SEGMENT12, 
                    target.SEGMENT13=src.SEGMENT13, 
                    target.SEGMENT14=src.SEGMENT14, 
                    target.SEGMENT15=src.SEGMENT15, 
                    target.SEGMENT2=src.SEGMENT2, 
                    target.SEGMENT3=src.SEGMENT3, 
                    target.SEGMENT4=src.SEGMENT4, 
                    target.SEGMENT5=src.SEGMENT5, 
                    target.SEGMENT6=src.SEGMENT6, 
                    target.SEGMENT7=src.SEGMENT7, 
                    target.SEGMENT8=src.SEGMENT8, 
                    target.SEGMENT9=src.SEGMENT9, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    BUDGETNUMBERKEY, 
                    CODE, 
                    DELETED, 
                    DESCRIPT, 
                    EFFDATE, 
                    EXPDATE, 
                    MODBY, 
                    MODDTTM, 
                    SEGMENT1, 
                    SEGMENT10, 
                    SEGMENT11, 
                    SEGMENT12, 
                    SEGMENT13, 
                    SEGMENT14, 
                    SEGMENT15, 
                    SEGMENT2, 
                    SEGMENT3, 
                    SEGMENT4, 
                    SEGMENT5, 
                    SEGMENT6, 
                    SEGMENT7, 
                    SEGMENT8, 
                    SEGMENT9, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BUDGETNUMBERKEY, 
                    src.CODE, 
                    src.DELETED, 
                    src.DESCRIPT, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.SEGMENT1, 
                    src.SEGMENT10, 
                    src.SEGMENT11, 
                    src.SEGMENT12, 
                    src.SEGMENT13, 
                    src.SEGMENT14, 
                    src.SEGMENT15, 
                    src.SEGMENT2, 
                    src.SEGMENT3, 
                    src.SEGMENT4, 
                    src.SEGMENT5, 
                    src.SEGMENT6, 
                    src.SEGMENT7, 
                    src.SEGMENT8, 
                    src.SEGMENT9, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_BUDGETNUMBER as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BUDGETNUMBERKEY, 
            strm.CODE, 
            strm.DELETED, 
            strm.DESCRIPT, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.SEGMENT1, 
            strm.SEGMENT10, 
            strm.SEGMENT11, 
            strm.SEGMENT12, 
            strm.SEGMENT13, 
            strm.SEGMENT14, 
            strm.SEGMENT15, 
            strm.SEGMENT2, 
            strm.SEGMENT3, 
            strm.SEGMENT4, 
            strm.SEGMENT5, 
            strm.SEGMENT6, 
            strm.SEGMENT7, 
            strm.SEGMENT8, 
            strm.SEGMENT9, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BUDGETNUMBERKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_BUDGETNUMBER as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BUDGETNUMBERKEY=src.BUDGETNUMBERKEY) OR (target.BUDGETNUMBERKEY IS NULL AND src.BUDGETNUMBERKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BUDGETNUMBERKEY=src.BUDGETNUMBERKEY, 
                    target.CODE=src.CODE, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.SEGMENT1=src.SEGMENT1, 
                    target.SEGMENT10=src.SEGMENT10, 
                    target.SEGMENT11=src.SEGMENT11, 
                    target.SEGMENT12=src.SEGMENT12, 
                    target.SEGMENT13=src.SEGMENT13, 
                    target.SEGMENT14=src.SEGMENT14, 
                    target.SEGMENT15=src.SEGMENT15, 
                    target.SEGMENT2=src.SEGMENT2, 
                    target.SEGMENT3=src.SEGMENT3, 
                    target.SEGMENT4=src.SEGMENT4, 
                    target.SEGMENT5=src.SEGMENT5, 
                    target.SEGMENT6=src.SEGMENT6, 
                    target.SEGMENT7=src.SEGMENT7, 
                    target.SEGMENT8=src.SEGMENT8, 
                    target.SEGMENT9=src.SEGMENT9, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, BUDGETNUMBERKEY, CODE, DELETED, DESCRIPT, EFFDATE, EXPDATE, MODBY, MODDTTM, SEGMENT1, SEGMENT10, SEGMENT11, SEGMENT12, SEGMENT13, SEGMENT14, SEGMENT15, SEGMENT2, SEGMENT3, SEGMENT4, SEGMENT5, SEGMENT6, SEGMENT7, SEGMENT8, SEGMENT9, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BUDGETNUMBERKEY, 
                    src.CODE, 
                    src.DELETED, 
                    src.DESCRIPT, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.SEGMENT1, 
                    src.SEGMENT10, 
                    src.SEGMENT11, 
                    src.SEGMENT12, 
                    src.SEGMENT13, 
                    src.SEGMENT14, 
                    src.SEGMENT15, 
                    src.SEGMENT2, 
                    src.SEGMENT3, 
                    src.SEGMENT4, 
                    src.SEGMENT5, 
                    src.SEGMENT6, 
                    src.SEGMENT7, 
                    src.SEGMENT8, 
                    src.SEGMENT9, 
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