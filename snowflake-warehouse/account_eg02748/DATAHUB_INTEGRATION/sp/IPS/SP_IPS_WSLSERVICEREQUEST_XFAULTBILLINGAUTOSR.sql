create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as target using (SELECT * FROM (SELECT 
            strm.ACTIVITYCODE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESS, 
            strm.DELETED, 
            strm.DISCONNECTIONDATE, 
            strm.DISCONNECTIONTYPE, 
            strm.IPSCOMPKEY, 
            strm.LEAKCUSTOMERSIDE, 
            strm.LEAKWATERCARESIDE, 
            strm.LOCATIONDESCRIPTION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEWMETERID, 
            strm.NEWMETERINSTALLDATE, 
            strm.NEWMETERREADING, 
            strm.OLDMETERID, 
            strm.OLDMETERREADING, 
            strm.ORIGINATEDSR, 
            strm.SRCREATED, 
            strm.VARIATION_ID, 
            strm.WORKORDER, 
            strm.XFAULTBILLINGAUTOSRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XFAULTBILLINGAUTOSRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XFAULTBILLINGAUTOSRKEY=src.XFAULTBILLINGAUTOSRKEY) OR (target.XFAULTBILLINGAUTOSRKEY IS NULL AND src.XFAULTBILLINGAUTOSRKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTIVITYCODE=src.ACTIVITYCODE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESS=src.ADDRESS, 
                    target.DELETED=src.DELETED, 
                    target.DISCONNECTIONDATE=src.DISCONNECTIONDATE, 
                    target.DISCONNECTIONTYPE=src.DISCONNECTIONTYPE, 
                    target.IPSCOMPKEY=src.IPSCOMPKEY, 
                    target.LEAKCUSTOMERSIDE=src.LEAKCUSTOMERSIDE, 
                    target.LEAKWATERCARESIDE=src.LEAKWATERCARESIDE, 
                    target.LOCATIONDESCRIPTION=src.LOCATIONDESCRIPTION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEWMETERID=src.NEWMETERID, 
                    target.NEWMETERINSTALLDATE=src.NEWMETERINSTALLDATE, 
                    target.NEWMETERREADING=src.NEWMETERREADING, 
                    target.OLDMETERID=src.OLDMETERID, 
                    target.OLDMETERREADING=src.OLDMETERREADING, 
                    target.ORIGINATEDSR=src.ORIGINATEDSR, 
                    target.SRCREATED=src.SRCREATED, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WORKORDER=src.WORKORDER, 
                    target.XFAULTBILLINGAUTOSRKEY=src.XFAULTBILLINGAUTOSRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTIVITYCODE, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRESS, 
                    DELETED, 
                    DISCONNECTIONDATE, 
                    DISCONNECTIONTYPE, 
                    IPSCOMPKEY, 
                    LEAKCUSTOMERSIDE, 
                    LEAKWATERCARESIDE, 
                    LOCATIONDESCRIPTION, 
                    MODBY, 
                    MODDTTM, 
                    NEWMETERID, 
                    NEWMETERINSTALLDATE, 
                    NEWMETERREADING, 
                    OLDMETERID, 
                    OLDMETERREADING, 
                    ORIGINATEDSR, 
                    SRCREATED, 
                    VARIATION_ID, 
                    WORKORDER, 
                    XFAULTBILLINGAUTOSRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTIVITYCODE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESS, 
                    src.DELETED, 
                    src.DISCONNECTIONDATE, 
                    src.DISCONNECTIONTYPE, 
                    src.IPSCOMPKEY, 
                    src.LEAKCUSTOMERSIDE, 
                    src.LEAKWATERCARESIDE, 
                    src.LOCATIONDESCRIPTION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEWMETERID, 
                    src.NEWMETERINSTALLDATE, 
                    src.NEWMETERREADING, 
                    src.OLDMETERID, 
                    src.OLDMETERREADING, 
                    src.ORIGINATEDSR, 
                    src.SRCREATED, 
                    src.VARIATION_ID, 
                    src.WORKORDER, 
                    src.XFAULTBILLINGAUTOSRKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as target using (
                SELECT * FROM (SELECT 
            strm.ACTIVITYCODE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESS, 
            strm.DELETED, 
            strm.DISCONNECTIONDATE, 
            strm.DISCONNECTIONTYPE, 
            strm.IPSCOMPKEY, 
            strm.LEAKCUSTOMERSIDE, 
            strm.LEAKWATERCARESIDE, 
            strm.LOCATIONDESCRIPTION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEWMETERID, 
            strm.NEWMETERINSTALLDATE, 
            strm.NEWMETERREADING, 
            strm.OLDMETERID, 
            strm.OLDMETERREADING, 
            strm.ORIGINATEDSR, 
            strm.SRCREATED, 
            strm.VARIATION_ID, 
            strm.WORKORDER, 
            strm.XFAULTBILLINGAUTOSRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XFAULTBILLINGAUTOSRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XFAULTBILLINGAUTOSRKEY=src.XFAULTBILLINGAUTOSRKEY) OR (target.XFAULTBILLINGAUTOSRKEY IS NULL AND src.XFAULTBILLINGAUTOSRKEY IS NULL)) 
                when matched then update set
                    target.ACTIVITYCODE=src.ACTIVITYCODE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESS=src.ADDRESS, 
                    target.DELETED=src.DELETED, 
                    target.DISCONNECTIONDATE=src.DISCONNECTIONDATE, 
                    target.DISCONNECTIONTYPE=src.DISCONNECTIONTYPE, 
                    target.IPSCOMPKEY=src.IPSCOMPKEY, 
                    target.LEAKCUSTOMERSIDE=src.LEAKCUSTOMERSIDE, 
                    target.LEAKWATERCARESIDE=src.LEAKWATERCARESIDE, 
                    target.LOCATIONDESCRIPTION=src.LOCATIONDESCRIPTION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEWMETERID=src.NEWMETERID, 
                    target.NEWMETERINSTALLDATE=src.NEWMETERINSTALLDATE, 
                    target.NEWMETERREADING=src.NEWMETERREADING, 
                    target.OLDMETERID=src.OLDMETERID, 
                    target.OLDMETERREADING=src.OLDMETERREADING, 
                    target.ORIGINATEDSR=src.ORIGINATEDSR, 
                    target.SRCREATED=src.SRCREATED, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WORKORDER=src.WORKORDER, 
                    target.XFAULTBILLINGAUTOSRKEY=src.XFAULTBILLINGAUTOSRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTIVITYCODE, ADDBY, ADDDTTM, ADDRESS, DELETED, DISCONNECTIONDATE, DISCONNECTIONTYPE, IPSCOMPKEY, LEAKCUSTOMERSIDE, LEAKWATERCARESIDE, LOCATIONDESCRIPTION, MODBY, MODDTTM, NEWMETERID, NEWMETERINSTALLDATE, NEWMETERREADING, OLDMETERID, OLDMETERREADING, ORIGINATEDSR, SRCREATED, VARIATION_ID, WORKORDER, XFAULTBILLINGAUTOSRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTIVITYCODE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESS, 
                    src.DELETED, 
                    src.DISCONNECTIONDATE, 
                    src.DISCONNECTIONTYPE, 
                    src.IPSCOMPKEY, 
                    src.LEAKCUSTOMERSIDE, 
                    src.LEAKWATERCARESIDE, 
                    src.LOCATIONDESCRIPTION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEWMETERID, 
                    src.NEWMETERINSTALLDATE, 
                    src.NEWMETERREADING, 
                    src.OLDMETERID, 
                    src.OLDMETERREADING, 
                    src.ORIGINATEDSR, 
                    src.SRCREATED, 
                    src.VARIATION_ID, 
                    src.WORKORDER, 
                    src.XFAULTBILLINGAUTOSRKEY,     
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