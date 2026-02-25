create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_PLANNING_PLANREVIEWTYPE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANREVIEWTYPE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_PLANNING_PLANREVIEWTYPE as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDCONDFRMLA, 
            strm.ADDDTTM, 
            strm.APPLANDEFNKEY, 
            strm.APPLANPROCESSSTATEKEY, 
            strm.APPLANREVIEWTYPEKEY, 
            strm.ASSGNGRPFRMLA, 
            strm.DATALAKE_DELETED, 
            strm.DEPT, 
            strm.DESCRIPT, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.INTERNALONLYFLAG, 
            strm.ISPUBLIC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLANCOPYNO, 
            strm.PORTALDESCRIPTION, 
            strm.REVIEWTYPE, 
            strm.SHOWINPORTAL, 
            strm.SUSPDAYS, 
            strm.VARIATION_ID, 
            strm.WORKDAY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APPLANREVIEWTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANREVIEWTYPE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APPLANREVIEWTYPEKEY=src.APPLANREVIEWTYPEKEY) OR (target.APPLANREVIEWTYPEKEY IS NULL AND src.APPLANREVIEWTYPEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDCONDFRMLA=src.ADDCONDFRMLA, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLANDEFNKEY=src.APPLANDEFNKEY, 
                    target.APPLANPROCESSSTATEKEY=src.APPLANPROCESSSTATEKEY, 
                    target.APPLANREVIEWTYPEKEY=src.APPLANREVIEWTYPEKEY, 
                    target.ASSGNGRPFRMLA=src.ASSGNGRPFRMLA, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPT=src.DEPT, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTERNALONLYFLAG=src.INTERNALONLYFLAG, 
                    target.ISPUBLIC=src.ISPUBLIC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLANCOPYNO=src.PLANCOPYNO, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.REVIEWTYPE=src.REVIEWTYPE, 
                    target.SHOWINPORTAL=src.SHOWINPORTAL, 
                    target.SUSPDAYS=src.SUSPDAYS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WORKDAY=src.WORKDAY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDCONDFRMLA, 
                    ADDDTTM, 
                    APPLANDEFNKEY, 
                    APPLANPROCESSSTATEKEY, 
                    APPLANREVIEWTYPEKEY, 
                    ASSGNGRPFRMLA, 
                    DATALAKE_DELETED, 
                    DEPT, 
                    DESCRIPT, 
                    DPDESC, 
                    EFFDATE, 
                    EXPDATE, 
                    INTERNALONLYFLAG, 
                    ISPUBLIC, 
                    MODBY, 
                    MODDTTM, 
                    PLANCOPYNO, 
                    PORTALDESCRIPTION, 
                    REVIEWTYPE, 
                    SHOWINPORTAL, 
                    SUSPDAYS, 
                    VARIATION_ID, 
                    WORKDAY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDCONDFRMLA, 
                    src.ADDDTTM, 
                    src.APPLANDEFNKEY, 
                    src.APPLANPROCESSSTATEKEY, 
                    src.APPLANREVIEWTYPEKEY, 
                    src.ASSGNGRPFRMLA, 
                    src.DATALAKE_DELETED, 
                    src.DEPT, 
                    src.DESCRIPT, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.INTERNALONLYFLAG, 
                    src.ISPUBLIC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLANCOPYNO, 
                    src.PORTALDESCRIPTION, 
                    src.REVIEWTYPE, 
                    src.SHOWINPORTAL, 
                    src.SUSPDAYS, 
                    src.VARIATION_ID, 
                    src.WORKDAY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PLANNING_PLANREVIEWTYPE as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDCONDFRMLA, 
            strm.ADDDTTM, 
            strm.APPLANDEFNKEY, 
            strm.APPLANPROCESSSTATEKEY, 
            strm.APPLANREVIEWTYPEKEY, 
            strm.ASSGNGRPFRMLA, 
            strm.DATALAKE_DELETED, 
            strm.DEPT, 
            strm.DESCRIPT, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.INTERNALONLYFLAG, 
            strm.ISPUBLIC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLANCOPYNO, 
            strm.PORTALDESCRIPTION, 
            strm.REVIEWTYPE, 
            strm.SHOWINPORTAL, 
            strm.SUSPDAYS, 
            strm.VARIATION_ID, 
            strm.WORKDAY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APPLANREVIEWTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANREVIEWTYPE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APPLANREVIEWTYPEKEY=src.APPLANREVIEWTYPEKEY) OR (target.APPLANREVIEWTYPEKEY IS NULL AND src.APPLANREVIEWTYPEKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDCONDFRMLA=src.ADDCONDFRMLA, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLANDEFNKEY=src.APPLANDEFNKEY, 
                    target.APPLANPROCESSSTATEKEY=src.APPLANPROCESSSTATEKEY, 
                    target.APPLANREVIEWTYPEKEY=src.APPLANREVIEWTYPEKEY, 
                    target.ASSGNGRPFRMLA=src.ASSGNGRPFRMLA, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPT=src.DEPT, 
                    target.DESCRIPT=src.DESCRIPT, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTERNALONLYFLAG=src.INTERNALONLYFLAG, 
                    target.ISPUBLIC=src.ISPUBLIC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLANCOPYNO=src.PLANCOPYNO, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.REVIEWTYPE=src.REVIEWTYPE, 
                    target.SHOWINPORTAL=src.SHOWINPORTAL, 
                    target.SUSPDAYS=src.SUSPDAYS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WORKDAY=src.WORKDAY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDCONDFRMLA, ADDDTTM, APPLANDEFNKEY, APPLANPROCESSSTATEKEY, APPLANREVIEWTYPEKEY, ASSGNGRPFRMLA, DATALAKE_DELETED, DEPT, DESCRIPT, DPDESC, EFFDATE, EXPDATE, INTERNALONLYFLAG, ISPUBLIC, MODBY, MODDTTM, PLANCOPYNO, PORTALDESCRIPTION, REVIEWTYPE, SHOWINPORTAL, SUSPDAYS, VARIATION_ID, WORKDAY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDCONDFRMLA, 
                    src.ADDDTTM, 
                    src.APPLANDEFNKEY, 
                    src.APPLANPROCESSSTATEKEY, 
                    src.APPLANREVIEWTYPEKEY, 
                    src.ASSGNGRPFRMLA, 
                    src.DATALAKE_DELETED, 
                    src.DEPT, 
                    src.DESCRIPT, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.INTERNALONLYFLAG, 
                    src.ISPUBLIC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLANCOPYNO, 
                    src.PORTALDESCRIPTION, 
                    src.REVIEWTYPE, 
                    src.SHOWINPORTAL, 
                    src.SUSPDAYS, 
                    src.VARIATION_ID, 
                    src.WORKDAY,     
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