create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTDIRECTDEBITKEY, 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTNAME, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BANKACCTNO, 
            strm.BANKNAME, 
            strm.CCADDRESS, 
            strm.COUNTRY, 
            strm.DATEINITIATE, 
            strm.DATEOFCORRESPONDENCE, 
            strm.DDFILENAME, 
            strm.DELETED, 
            strm.DIRECTDEBITLETTERKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTADD1, 
            strm.NOTADD2, 
            strm.NOTADD3, 
            strm.NOTADD4, 
            strm.NOTADD5, 
            strm.POSTCODE, 
            strm.PROPADDRESS, 
            strm.SDLDATE, 
            strm.SERVICEREQUESTNUMBER, 
            strm.TOADDRESS, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITLETTERKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITLETTERKEY=src.DIRECTDEBITLETTERKEY) OR (target.DIRECTDEBITLETTERKEY IS NULL AND src.DIRECTDEBITLETTERKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTDIRECTDEBITKEY=src.ACCOUNTDIRECTDEBITKEY, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTNAME=src.ACCOUNTNAME, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BANKACCTNO=src.BANKACCTNO, 
                    target.BANKNAME=src.BANKNAME, 
                    target.CCADDRESS=src.CCADDRESS, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DATEINITIATE=src.DATEINITIATE, 
                    target.DATEOFCORRESPONDENCE=src.DATEOFCORRESPONDENCE, 
                    target.DDFILENAME=src.DDFILENAME, 
                    target.DELETED=src.DELETED, 
                    target.DIRECTDEBITLETTERKEY=src.DIRECTDEBITLETTERKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTADD1=src.NOTADD1, 
                    target.NOTADD2=src.NOTADD2, 
                    target.NOTADD3=src.NOTADD3, 
                    target.NOTADD4=src.NOTADD4, 
                    target.NOTADD5=src.NOTADD5, 
                    target.POSTCODE=src.POSTCODE, 
                    target.PROPADDRESS=src.PROPADDRESS, 
                    target.SDLDATE=src.SDLDATE, 
                    target.SERVICEREQUESTNUMBER=src.SERVICEREQUESTNUMBER, 
                    target.TOADDRESS=src.TOADDRESS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTDIRECTDEBITKEY, 
                    ACCOUNTKEY, 
                    ACCOUNTNAME, 
                    ADDBY, 
                    ADDDTTM, 
                    BANKACCTNO, 
                    BANKNAME, 
                    CCADDRESS, 
                    COUNTRY, 
                    DATEINITIATE, 
                    DATEOFCORRESPONDENCE, 
                    DDFILENAME, 
                    DELETED, 
                    DIRECTDEBITLETTERKEY, 
                    MODBY, 
                    MODDTTM, 
                    NOTADD1, 
                    NOTADD2, 
                    NOTADD3, 
                    NOTADD4, 
                    NOTADD5, 
                    POSTCODE, 
                    PROPADDRESS, 
                    SDLDATE, 
                    SERVICEREQUESTNUMBER, 
                    TOADDRESS, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTDIRECTDEBITKEY, 
                    src.ACCOUNTKEY, 
                    src.ACCOUNTNAME, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BANKACCTNO, 
                    src.BANKNAME, 
                    src.CCADDRESS, 
                    src.COUNTRY, 
                    src.DATEINITIATE, 
                    src.DATEOFCORRESPONDENCE, 
                    src.DDFILENAME, 
                    src.DELETED, 
                    src.DIRECTDEBITLETTERKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTADD1, 
                    src.NOTADD2, 
                    src.NOTADD3, 
                    src.NOTADD4, 
                    src.NOTADD5, 
                    src.POSTCODE, 
                    src.PROPADDRESS, 
                    src.SDLDATE, 
                    src.SERVICEREQUESTNUMBER, 
                    src.TOADDRESS, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLDIRECTDEBIT_DIRECTDEBITLETTER as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTDIRECTDEBITKEY, 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTNAME, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BANKACCTNO, 
            strm.BANKNAME, 
            strm.CCADDRESS, 
            strm.COUNTRY, 
            strm.DATEINITIATE, 
            strm.DATEOFCORRESPONDENCE, 
            strm.DDFILENAME, 
            strm.DELETED, 
            strm.DIRECTDEBITLETTERKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTADD1, 
            strm.NOTADD2, 
            strm.NOTADD3, 
            strm.NOTADD4, 
            strm.NOTADD5, 
            strm.POSTCODE, 
            strm.PROPADDRESS, 
            strm.SDLDATE, 
            strm.SERVICEREQUESTNUMBER, 
            strm.TOADDRESS, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITLETTERKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLDIRECTDEBIT_DIRECTDEBITLETTER as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITLETTERKEY=src.DIRECTDEBITLETTERKEY) OR (target.DIRECTDEBITLETTERKEY IS NULL AND src.DIRECTDEBITLETTERKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTDIRECTDEBITKEY=src.ACCOUNTDIRECTDEBITKEY, 
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTNAME=src.ACCOUNTNAME, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BANKACCTNO=src.BANKACCTNO, 
                    target.BANKNAME=src.BANKNAME, 
                    target.CCADDRESS=src.CCADDRESS, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DATEINITIATE=src.DATEINITIATE, 
                    target.DATEOFCORRESPONDENCE=src.DATEOFCORRESPONDENCE, 
                    target.DDFILENAME=src.DDFILENAME, 
                    target.DELETED=src.DELETED, 
                    target.DIRECTDEBITLETTERKEY=src.DIRECTDEBITLETTERKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTADD1=src.NOTADD1, 
                    target.NOTADD2=src.NOTADD2, 
                    target.NOTADD3=src.NOTADD3, 
                    target.NOTADD4=src.NOTADD4, 
                    target.NOTADD5=src.NOTADD5, 
                    target.POSTCODE=src.POSTCODE, 
                    target.PROPADDRESS=src.PROPADDRESS, 
                    target.SDLDATE=src.SDLDATE, 
                    target.SERVICEREQUESTNUMBER=src.SERVICEREQUESTNUMBER, 
                    target.TOADDRESS=src.TOADDRESS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTDIRECTDEBITKEY, ACCOUNTKEY, ACCOUNTNAME, ADDBY, ADDDTTM, BANKACCTNO, BANKNAME, CCADDRESS, COUNTRY, DATEINITIATE, DATEOFCORRESPONDENCE, DDFILENAME, DELETED, DIRECTDEBITLETTERKEY, MODBY, MODDTTM, NOTADD1, NOTADD2, NOTADD3, NOTADD4, NOTADD5, POSTCODE, PROPADDRESS, SDLDATE, SERVICEREQUESTNUMBER, TOADDRESS, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTDIRECTDEBITKEY, 
                    src.ACCOUNTKEY, 
                    src.ACCOUNTNAME, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BANKACCTNO, 
                    src.BANKNAME, 
                    src.CCADDRESS, 
                    src.COUNTRY, 
                    src.DATEINITIATE, 
                    src.DATEOFCORRESPONDENCE, 
                    src.DDFILENAME, 
                    src.DELETED, 
                    src.DIRECTDEBITLETTERKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTADD1, 
                    src.NOTADD2, 
                    src.NOTADD3, 
                    src.NOTADD4, 
                    src.NOTADD5, 
                    src.POSTCODE, 
                    src.PROPADDRESS, 
                    src.SDLDATE, 
                    src.SERVICEREQUESTNUMBER, 
                    src.TOADDRESS, 
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