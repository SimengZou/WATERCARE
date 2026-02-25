create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLBILLING_SENDBILLTODELETED()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_SENDBILLTODELETED_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLBILLING_SENDBILLTODELETED as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDR1, 
            strm.ADDR2, 
            strm.ADDRESSCONTACTKEY, 
            strm.CARRT, 
            strm.CASSBARCODE, 
            strm.CASSVER, 
            strm.CITY, 
            strm.COUNTRY, 
            strm.DELETED, 
            strm.DELIVERYOPTION, 
            strm.DPC, 
            strm.EFFECTIVEDATE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ORIGADDBY, 
            strm.ORIGADDDTTM, 
            strm.ORIGMODBY, 
            strm.ORIGMODDTTM, 
            strm.SEASADDR1, 
            strm.SEASADDR2, 
            strm.SEASCARRT, 
            strm.SEASCASSVER, 
            strm.SEASCITY, 
            strm.SEASCOUNTRY, 
            strm.SEASDPC, 
            strm.SEASFROMDATE, 
            strm.SEASLOT, 
            strm.SEASSTATE, 
            strm.SEASTODATE, 
            strm.SEASZIP, 
            strm.SENDBILLTODELETEDKEY, 
            strm.SENDBILLTOKEY, 
            strm.SENDTOLINE1, 
            strm.SENDTOLINE2, 
            strm.STATE, 
            strm.VARIATION_ID, 
            strm.ZIP, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SENDBILLTODELETEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_SENDBILLTODELETED as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SENDBILLTODELETEDKEY=src.SENDBILLTODELETEDKEY) OR (target.SENDBILLTODELETEDKEY IS NULL AND src.SENDBILLTODELETEDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDR1=src.ADDR1, 
                    target.ADDR2=src.ADDR2, 
                    target.ADDRESSCONTACTKEY=src.ADDRESSCONTACTKEY, 
                    target.CARRT=src.CARRT, 
                    target.CASSBARCODE=src.CASSBARCODE, 
                    target.CASSVER=src.CASSVER, 
                    target.CITY=src.CITY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DELETED=src.DELETED, 
                    target.DELIVERYOPTION=src.DELIVERYOPTION, 
                    target.DPC=src.DPC, 
                    target.EFFECTIVEDATE=src.EFFECTIVEDATE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ORIGADDBY=src.ORIGADDBY, 
                    target.ORIGADDDTTM=src.ORIGADDDTTM, 
                    target.ORIGMODBY=src.ORIGMODBY, 
                    target.ORIGMODDTTM=src.ORIGMODDTTM, 
                    target.SEASADDR1=src.SEASADDR1, 
                    target.SEASADDR2=src.SEASADDR2, 
                    target.SEASCARRT=src.SEASCARRT, 
                    target.SEASCASSVER=src.SEASCASSVER, 
                    target.SEASCITY=src.SEASCITY, 
                    target.SEASCOUNTRY=src.SEASCOUNTRY, 
                    target.SEASDPC=src.SEASDPC, 
                    target.SEASFROMDATE=src.SEASFROMDATE, 
                    target.SEASLOT=src.SEASLOT, 
                    target.SEASSTATE=src.SEASSTATE, 
                    target.SEASTODATE=src.SEASTODATE, 
                    target.SEASZIP=src.SEASZIP, 
                    target.SENDBILLTODELETEDKEY=src.SENDBILLTODELETEDKEY, 
                    target.SENDBILLTOKEY=src.SENDBILLTOKEY, 
                    target.SENDTOLINE1=src.SENDTOLINE1, 
                    target.SENDTOLINE2=src.SENDTOLINE2, 
                    target.STATE=src.STATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ZIP=src.ZIP, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDR1, 
                    ADDR2, 
                    ADDRESSCONTACTKEY, 
                    CARRT, 
                    CASSBARCODE, 
                    CASSVER, 
                    CITY, 
                    COUNTRY, 
                    DELETED, 
                    DELIVERYOPTION, 
                    DPC, 
                    EFFECTIVEDATE, 
                    LOT, 
                    MODBY, 
                    MODDTTM, 
                    ORIGADDBY, 
                    ORIGADDDTTM, 
                    ORIGMODBY, 
                    ORIGMODDTTM, 
                    SEASADDR1, 
                    SEASADDR2, 
                    SEASCARRT, 
                    SEASCASSVER, 
                    SEASCITY, 
                    SEASCOUNTRY, 
                    SEASDPC, 
                    SEASFROMDATE, 
                    SEASLOT, 
                    SEASSTATE, 
                    SEASTODATE, 
                    SEASZIP, 
                    SENDBILLTODELETEDKEY, 
                    SENDBILLTOKEY, 
                    SENDTOLINE1, 
                    SENDTOLINE2, 
                    STATE, 
                    VARIATION_ID, 
                    ZIP, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDR1, 
                    src.ADDR2, 
                    src.ADDRESSCONTACTKEY, 
                    src.CARRT, 
                    src.CASSBARCODE, 
                    src.CASSVER, 
                    src.CITY, 
                    src.COUNTRY, 
                    src.DELETED, 
                    src.DELIVERYOPTION, 
                    src.DPC, 
                    src.EFFECTIVEDATE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ORIGADDBY, 
                    src.ORIGADDDTTM, 
                    src.ORIGMODBY, 
                    src.ORIGMODDTTM, 
                    src.SEASADDR1, 
                    src.SEASADDR2, 
                    src.SEASCARRT, 
                    src.SEASCASSVER, 
                    src.SEASCITY, 
                    src.SEASCOUNTRY, 
                    src.SEASDPC, 
                    src.SEASFROMDATE, 
                    src.SEASLOT, 
                    src.SEASSTATE, 
                    src.SEASTODATE, 
                    src.SEASZIP, 
                    src.SENDBILLTODELETEDKEY, 
                    src.SENDBILLTOKEY, 
                    src.SENDTOLINE1, 
                    src.SENDTOLINE2, 
                    src.STATE, 
                    src.VARIATION_ID, 
                    src.ZIP,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLBILLING_SENDBILLTODELETED as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDR1, 
            strm.ADDR2, 
            strm.ADDRESSCONTACTKEY, 
            strm.CARRT, 
            strm.CASSBARCODE, 
            strm.CASSVER, 
            strm.CITY, 
            strm.COUNTRY, 
            strm.DELETED, 
            strm.DELIVERYOPTION, 
            strm.DPC, 
            strm.EFFECTIVEDATE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ORIGADDBY, 
            strm.ORIGADDDTTM, 
            strm.ORIGMODBY, 
            strm.ORIGMODDTTM, 
            strm.SEASADDR1, 
            strm.SEASADDR2, 
            strm.SEASCARRT, 
            strm.SEASCASSVER, 
            strm.SEASCITY, 
            strm.SEASCOUNTRY, 
            strm.SEASDPC, 
            strm.SEASFROMDATE, 
            strm.SEASLOT, 
            strm.SEASSTATE, 
            strm.SEASTODATE, 
            strm.SEASZIP, 
            strm.SENDBILLTODELETEDKEY, 
            strm.SENDBILLTOKEY, 
            strm.SENDTOLINE1, 
            strm.SENDTOLINE2, 
            strm.STATE, 
            strm.VARIATION_ID, 
            strm.ZIP, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SENDBILLTODELETEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_SENDBILLTODELETED as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SENDBILLTODELETEDKEY=src.SENDBILLTODELETEDKEY) OR (target.SENDBILLTODELETEDKEY IS NULL AND src.SENDBILLTODELETEDKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDR1=src.ADDR1, 
                    target.ADDR2=src.ADDR2, 
                    target.ADDRESSCONTACTKEY=src.ADDRESSCONTACTKEY, 
                    target.CARRT=src.CARRT, 
                    target.CASSBARCODE=src.CASSBARCODE, 
                    target.CASSVER=src.CASSVER, 
                    target.CITY=src.CITY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DELETED=src.DELETED, 
                    target.DELIVERYOPTION=src.DELIVERYOPTION, 
                    target.DPC=src.DPC, 
                    target.EFFECTIVEDATE=src.EFFECTIVEDATE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ORIGADDBY=src.ORIGADDBY, 
                    target.ORIGADDDTTM=src.ORIGADDDTTM, 
                    target.ORIGMODBY=src.ORIGMODBY, 
                    target.ORIGMODDTTM=src.ORIGMODDTTM, 
                    target.SEASADDR1=src.SEASADDR1, 
                    target.SEASADDR2=src.SEASADDR2, 
                    target.SEASCARRT=src.SEASCARRT, 
                    target.SEASCASSVER=src.SEASCASSVER, 
                    target.SEASCITY=src.SEASCITY, 
                    target.SEASCOUNTRY=src.SEASCOUNTRY, 
                    target.SEASDPC=src.SEASDPC, 
                    target.SEASFROMDATE=src.SEASFROMDATE, 
                    target.SEASLOT=src.SEASLOT, 
                    target.SEASSTATE=src.SEASSTATE, 
                    target.SEASTODATE=src.SEASTODATE, 
                    target.SEASZIP=src.SEASZIP, 
                    target.SENDBILLTODELETEDKEY=src.SENDBILLTODELETEDKEY, 
                    target.SENDBILLTOKEY=src.SENDBILLTOKEY, 
                    target.SENDTOLINE1=src.SENDTOLINE1, 
                    target.SENDTOLINE2=src.SENDTOLINE2, 
                    target.STATE=src.STATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ZIP=src.ZIP, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ADDBY, ADDDTTM, ADDR1, ADDR2, ADDRESSCONTACTKEY, CARRT, CASSBARCODE, CASSVER, CITY, COUNTRY, DELETED, DELIVERYOPTION, DPC, EFFECTIVEDATE, LOT, MODBY, MODDTTM, ORIGADDBY, ORIGADDDTTM, ORIGMODBY, ORIGMODDTTM, SEASADDR1, SEASADDR2, SEASCARRT, SEASCASSVER, SEASCITY, SEASCOUNTRY, SEASDPC, SEASFROMDATE, SEASLOT, SEASSTATE, SEASTODATE, SEASZIP, SENDBILLTODELETEDKEY, SENDBILLTOKEY, SENDTOLINE1, SENDTOLINE2, STATE, VARIATION_ID, ZIP, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDR1, 
                    src.ADDR2, 
                    src.ADDRESSCONTACTKEY, 
                    src.CARRT, 
                    src.CASSBARCODE, 
                    src.CASSVER, 
                    src.CITY, 
                    src.COUNTRY, 
                    src.DELETED, 
                    src.DELIVERYOPTION, 
                    src.DPC, 
                    src.EFFECTIVEDATE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ORIGADDBY, 
                    src.ORIGADDDTTM, 
                    src.ORIGMODBY, 
                    src.ORIGMODDTTM, 
                    src.SEASADDR1, 
                    src.SEASADDR2, 
                    src.SEASCARRT, 
                    src.SEASCASSVER, 
                    src.SEASCITY, 
                    src.SEASCOUNTRY, 
                    src.SEASDPC, 
                    src.SEASFROMDATE, 
                    src.SEASLOT, 
                    src.SEASSTATE, 
                    src.SEASTODATE, 
                    src.SEASZIP, 
                    src.SENDBILLTODELETEDKEY, 
                    src.SENDBILLTOKEY, 
                    src.SENDTOLINE1, 
                    src.SENDTOLINE2, 
                    src.STATE, 
                    src.VARIATION_ID, 
                    src.ZIP,     
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