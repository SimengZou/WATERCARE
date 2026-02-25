create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_APFEE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_APFEE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_APFEE as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMT, 
            strm.APFEEKEY, 
            strm.APFEETYPEKEY, 
            strm.APKEY, 
            strm.APPLDBY, 
            strm.APPLDDTTM, 
            strm.BGTNOKEY, 
            strm.BILLACCTKEY, 
            strm.BILLNO, 
            strm.CDRPRODFAMILY, 
            strm.CMPTRGEN, 
            strm.DELETED, 
            strm.FEECODE, 
            strm.FEEDESC, 
            strm.FEETYPEVERSIONKEY, 
            strm.HIDEINAPPL, 
            strm.INITDEPFEEKEY, 
            strm.LIENED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAIDDTTM, 
            strm.PAYORD, 
            strm.PENRUNKEY, 
            strm.PNAPFEEKEY, 
            strm.PNRVRUNKEY, 
            strm.QTY, 
            strm.REFUNDABLE, 
            strm.SRCBGTNOKEY, 
            strm.STAT, 
            strm.VAL, 
            strm.VARIATION_ID, 
            strm.WAIVABLE, 
            strm.WAIVED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APFEEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APFEE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APFEEKEY=src.APFEEKEY) OR (target.APFEEKEY IS NULL AND src.APFEEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMT=src.AMT, 
                    target.APFEEKEY=src.APFEEKEY, 
                    target.APFEETYPEKEY=src.APFEETYPEKEY, 
                    target.APKEY=src.APKEY, 
                    target.APPLDBY=src.APPLDBY, 
                    target.APPLDDTTM=src.APPLDDTTM, 
                    target.BGTNOKEY=src.BGTNOKEY, 
                    target.BILLACCTKEY=src.BILLACCTKEY, 
                    target.BILLNO=src.BILLNO, 
                    target.CDRPRODFAMILY=src.CDRPRODFAMILY, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.DELETED=src.DELETED, 
                    target.FEECODE=src.FEECODE, 
                    target.FEEDESC=src.FEEDESC, 
                    target.FEETYPEVERSIONKEY=src.FEETYPEVERSIONKEY, 
                    target.HIDEINAPPL=src.HIDEINAPPL, 
                    target.INITDEPFEEKEY=src.INITDEPFEEKEY, 
                    target.LIENED=src.LIENED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAIDDTTM=src.PAIDDTTM, 
                    target.PAYORD=src.PAYORD, 
                    target.PENRUNKEY=src.PENRUNKEY, 
                    target.PNAPFEEKEY=src.PNAPFEEKEY, 
                    target.PNRVRUNKEY=src.PNRVRUNKEY, 
                    target.QTY=src.QTY, 
                    target.REFUNDABLE=src.REFUNDABLE, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.STAT=src.STAT, 
                    target.VAL=src.VAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WAIVABLE=src.WAIVABLE, 
                    target.WAIVED=src.WAIVED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AMT, 
                    APFEEKEY, 
                    APFEETYPEKEY, 
                    APKEY, 
                    APPLDBY, 
                    APPLDDTTM, 
                    BGTNOKEY, 
                    BILLACCTKEY, 
                    BILLNO, 
                    CDRPRODFAMILY, 
                    CMPTRGEN, 
                    DELETED, 
                    FEECODE, 
                    FEEDESC, 
                    FEETYPEVERSIONKEY, 
                    HIDEINAPPL, 
                    INITDEPFEEKEY, 
                    LIENED, 
                    MODBY, 
                    MODDTTM, 
                    PAIDDTTM, 
                    PAYORD, 
                    PENRUNKEY, 
                    PNAPFEEKEY, 
                    PNRVRUNKEY, 
                    QTY, 
                    REFUNDABLE, 
                    SRCBGTNOKEY, 
                    STAT, 
                    VAL, 
                    VARIATION_ID, 
                    WAIVABLE, 
                    WAIVED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMT, 
                    src.APFEEKEY, 
                    src.APFEETYPEKEY, 
                    src.APKEY, 
                    src.APPLDBY, 
                    src.APPLDDTTM, 
                    src.BGTNOKEY, 
                    src.BILLACCTKEY, 
                    src.BILLNO, 
                    src.CDRPRODFAMILY, 
                    src.CMPTRGEN, 
                    src.DELETED, 
                    src.FEECODE, 
                    src.FEEDESC, 
                    src.FEETYPEVERSIONKEY, 
                    src.HIDEINAPPL, 
                    src.INITDEPFEEKEY, 
                    src.LIENED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAIDDTTM, 
                    src.PAYORD, 
                    src.PENRUNKEY, 
                    src.PNAPFEEKEY, 
                    src.PNRVRUNKEY, 
                    src.QTY, 
                    src.REFUNDABLE, 
                    src.SRCBGTNOKEY, 
                    src.STAT, 
                    src.VAL, 
                    src.VARIATION_ID, 
                    src.WAIVABLE, 
                    src.WAIVED,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_APFEE as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMT, 
            strm.APFEEKEY, 
            strm.APFEETYPEKEY, 
            strm.APKEY, 
            strm.APPLDBY, 
            strm.APPLDDTTM, 
            strm.BGTNOKEY, 
            strm.BILLACCTKEY, 
            strm.BILLNO, 
            strm.CDRPRODFAMILY, 
            strm.CMPTRGEN, 
            strm.DELETED, 
            strm.FEECODE, 
            strm.FEEDESC, 
            strm.FEETYPEVERSIONKEY, 
            strm.HIDEINAPPL, 
            strm.INITDEPFEEKEY, 
            strm.LIENED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAIDDTTM, 
            strm.PAYORD, 
            strm.PENRUNKEY, 
            strm.PNAPFEEKEY, 
            strm.PNRVRUNKEY, 
            strm.QTY, 
            strm.REFUNDABLE, 
            strm.SRCBGTNOKEY, 
            strm.STAT, 
            strm.VAL, 
            strm.VARIATION_ID, 
            strm.WAIVABLE, 
            strm.WAIVED, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APFEEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APFEE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APFEEKEY=src.APFEEKEY) OR (target.APFEEKEY IS NULL AND src.APFEEKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMT=src.AMT, 
                    target.APFEEKEY=src.APFEEKEY, 
                    target.APFEETYPEKEY=src.APFEETYPEKEY, 
                    target.APKEY=src.APKEY, 
                    target.APPLDBY=src.APPLDBY, 
                    target.APPLDDTTM=src.APPLDDTTM, 
                    target.BGTNOKEY=src.BGTNOKEY, 
                    target.BILLACCTKEY=src.BILLACCTKEY, 
                    target.BILLNO=src.BILLNO, 
                    target.CDRPRODFAMILY=src.CDRPRODFAMILY, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.DELETED=src.DELETED, 
                    target.FEECODE=src.FEECODE, 
                    target.FEEDESC=src.FEEDESC, 
                    target.FEETYPEVERSIONKEY=src.FEETYPEVERSIONKEY, 
                    target.HIDEINAPPL=src.HIDEINAPPL, 
                    target.INITDEPFEEKEY=src.INITDEPFEEKEY, 
                    target.LIENED=src.LIENED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAIDDTTM=src.PAIDDTTM, 
                    target.PAYORD=src.PAYORD, 
                    target.PENRUNKEY=src.PENRUNKEY, 
                    target.PNAPFEEKEY=src.PNAPFEEKEY, 
                    target.PNRVRUNKEY=src.PNRVRUNKEY, 
                    target.QTY=src.QTY, 
                    target.REFUNDABLE=src.REFUNDABLE, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.STAT=src.STAT, 
                    target.VAL=src.VAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WAIVABLE=src.WAIVABLE, 
                    target.WAIVED=src.WAIVED, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AMT, APFEEKEY, APFEETYPEKEY, APKEY, APPLDBY, APPLDDTTM, BGTNOKEY, BILLACCTKEY, BILLNO, CDRPRODFAMILY, CMPTRGEN, DELETED, FEECODE, FEEDESC, FEETYPEVERSIONKEY, HIDEINAPPL, INITDEPFEEKEY, LIENED, MODBY, MODDTTM, PAIDDTTM, PAYORD, PENRUNKEY, PNAPFEEKEY, PNRVRUNKEY, QTY, REFUNDABLE, SRCBGTNOKEY, STAT, VAL, VARIATION_ID, WAIVABLE, WAIVED, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMT, 
                    src.APFEEKEY, 
                    src.APFEETYPEKEY, 
                    src.APKEY, 
                    src.APPLDBY, 
                    src.APPLDDTTM, 
                    src.BGTNOKEY, 
                    src.BILLACCTKEY, 
                    src.BILLNO, 
                    src.CDRPRODFAMILY, 
                    src.CMPTRGEN, 
                    src.DELETED, 
                    src.FEECODE, 
                    src.FEEDESC, 
                    src.FEETYPEVERSIONKEY, 
                    src.HIDEINAPPL, 
                    src.INITDEPFEEKEY, 
                    src.LIENED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAIDDTTM, 
                    src.PAYORD, 
                    src.PENRUNKEY, 
                    src.PNAPFEEKEY, 
                    src.PNRVRUNKEY, 
                    src.QTY, 
                    src.REFUNDABLE, 
                    src.SRCBGTNOKEY, 
                    src.STAT, 
                    src.VAL, 
                    src.VARIATION_ID, 
                    src.WAIVABLE, 
                    src.WAIVED,     
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