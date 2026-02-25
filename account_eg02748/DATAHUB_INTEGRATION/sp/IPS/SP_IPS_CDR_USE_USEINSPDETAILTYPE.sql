create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_USE_USEINSPDETAILTYPE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEINSPDETAILTYPE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_USE_USEINSPDETAILTYPE as target using (SELECT * FROM (SELECT 
            strm.ACCESSID, 
            strm.ADDBY, 
            strm.ADDCONDFRMLA, 
            strm.ADDDTTM, 
            strm.APUSEINSPDTLTYPEKEY, 
            strm.APUSEINSPTYPEKEY, 
            strm.CLASSNAME, 
            strm.DELETED, 
            strm.DESCRIPT, 
            strm.DISPLAY, 
            strm.DISPLAYORDER, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAGEPATH, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEINSPDTLTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEINSPDETAILTYPE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEINSPDTLTYPEKEY=src.APUSEINSPDTLTYPEKEY) OR (target.APUSEINSPDTLTYPEKEY IS NULL AND src.APUSEINSPDTLTYPEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCESSID=src.ACCESSID, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDCONDFRMLA=src.ADDCONDFRMLA, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEINSPDTLTYPEKEY=src.APUSEINSPDTLTYPEKEY, 
                    target.APUSEINSPTYPEKEY=src.APUSEINSPTYPEKEY, 
                    target.CLASSNAME=src.CLASSNAME, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.DISPLAY=src.DISPLAY, 
                    target.DISPLAYORDER=src.DISPLAYORDER, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAGEPATH=src.PAGEPATH, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCESSID, 
                    ADDBY, 
                    ADDCONDFRMLA, 
                    ADDDTTM, 
                    APUSEINSPDTLTYPEKEY, 
                    APUSEINSPTYPEKEY, 
                    CLASSNAME, 
                    DELETED, 
                    DESCRIPT, 
                    DISPLAY, 
                    DISPLAYORDER, 
                    EFFDATE, 
                    EXPDATE, 
                    MODBY, 
                    MODDTTM, 
                    PAGEPATH, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCESSID, 
                    src.ADDBY, 
                    src.ADDCONDFRMLA, 
                    src.ADDDTTM, 
                    src.APUSEINSPDTLTYPEKEY, 
                    src.APUSEINSPTYPEKEY, 
                    src.CLASSNAME, 
                    src.DELETED, 
                    src.DESCRIPT, 
                    src.DISPLAY, 
                    src.DISPLAYORDER, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAGEPATH, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_USE_USEINSPDETAILTYPE as target using (
                SELECT * FROM (SELECT 
            strm.ACCESSID, 
            strm.ADDBY, 
            strm.ADDCONDFRMLA, 
            strm.ADDDTTM, 
            strm.APUSEINSPDTLTYPEKEY, 
            strm.APUSEINSPTYPEKEY, 
            strm.CLASSNAME, 
            strm.DELETED, 
            strm.DESCRIPT, 
            strm.DISPLAY, 
            strm.DISPLAYORDER, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAGEPATH, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEINSPDTLTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEINSPDETAILTYPE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEINSPDTLTYPEKEY=src.APUSEINSPDTLTYPEKEY) OR (target.APUSEINSPDTLTYPEKEY IS NULL AND src.APUSEINSPDTLTYPEKEY IS NULL)) 
                when matched then update set
                    target.ACCESSID=src.ACCESSID, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDCONDFRMLA=src.ADDCONDFRMLA, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEINSPDTLTYPEKEY=src.APUSEINSPDTLTYPEKEY, 
                    target.APUSEINSPTYPEKEY=src.APUSEINSPTYPEKEY, 
                    target.CLASSNAME=src.CLASSNAME, 
                    target.DELETED=src.DELETED, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.DISPLAY=src.DISPLAY, 
                    target.DISPLAYORDER=src.DISPLAYORDER, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAGEPATH=src.PAGEPATH, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCESSID, ADDBY, ADDCONDFRMLA, ADDDTTM, APUSEINSPDTLTYPEKEY, APUSEINSPTYPEKEY, CLASSNAME, DELETED, DESCRIPT, DISPLAY, DISPLAYORDER, EFFDATE, EXPDATE, MODBY, MODDTTM, PAGEPATH, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCESSID, 
                    src.ADDBY, 
                    src.ADDCONDFRMLA, 
                    src.ADDDTTM, 
                    src.APUSEINSPDTLTYPEKEY, 
                    src.APUSEINSPTYPEKEY, 
                    src.CLASSNAME, 
                    src.DELETED, 
                    src.DESCRIPT, 
                    src.DISPLAY, 
                    src.DISPLAYORDER, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAGEPATH, 
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