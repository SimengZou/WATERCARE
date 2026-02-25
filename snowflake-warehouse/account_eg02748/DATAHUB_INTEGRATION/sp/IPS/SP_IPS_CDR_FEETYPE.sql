create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_FEETYPE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_FEETYPE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_FEETYPE as target using (SELECT * FROM (SELECT 
            strm.ACCTPREFIX, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ATCOSTUSEDFOR, 
            strm.AUTOPAY, 
            strm.CDRPRODUCTFAMILY, 
            strm.DELETED, 
            strm.DESTBGTNOKEY, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FEECLASS, 
            strm.FEECODE, 
            strm.FEEDESC, 
            strm.FEEGROUP, 
            strm.FEETYPEKEY, 
            strm.INITIALDEPFEETYPEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ORDERING, 
            strm.PAYMENTMETHODFLAG, 
            strm.PAYORD, 
            strm.PORTALDESCRIPTION, 
            strm.REFUNDABLE, 
            strm.SRCBGTNOKEY, 
            strm.SURC, 
            strm.USEINJOBESTIMATION, 
            strm.VARIATION_ID, 
            strm.VERSIONDATEBIND, 
            strm.VERSIONDATEMONIKER, 
            strm.WAIVABLE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FEETYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_FEETYPE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FEETYPEKEY=src.FEETYPEKEY) OR (target.FEETYPEKEY IS NULL AND src.FEETYPEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCTPREFIX=src.ACCTPREFIX, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ATCOSTUSEDFOR=src.ATCOSTUSEDFOR, 
                    target.AUTOPAY=src.AUTOPAY, 
                    target.CDRPRODUCTFAMILY=src.CDRPRODUCTFAMILY, 
                    target.DELETED=src.DELETED, 
                    target.DESTBGTNOKEY=src.DESTBGTNOKEY, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FEECLASS=src.FEECLASS, 
                    target.FEECODE=src.FEECODE, 
                    target.FEEDESC=src.FEEDESC, 
                    target.FEEGROUP=src.FEEGROUP, 
                    target.FEETYPEKEY=src.FEETYPEKEY, 
                    target.INITIALDEPFEETYPEKEY=src.INITIALDEPFEETYPEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ORDERING=src.ORDERING, 
                    target.PAYMENTMETHODFLAG=src.PAYMENTMETHODFLAG, 
                    target.PAYORD=src.PAYORD, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.REFUNDABLE=src.REFUNDABLE, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.SURC=src.SURC, 
                    target.USEINJOBESTIMATION=src.USEINJOBESTIMATION, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSIONDATEBIND=src.VERSIONDATEBIND, 
                    target.VERSIONDATEMONIKER=src.VERSIONDATEMONIKER, 
                    target.WAIVABLE=src.WAIVABLE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCTPREFIX, 
                    ADDBY, 
                    ADDDTTM, 
                    ATCOSTUSEDFOR, 
                    AUTOPAY, 
                    CDRPRODUCTFAMILY, 
                    DELETED, 
                    DESTBGTNOKEY, 
                    DPDESC, 
                    EFFDATE, 
                    EXPDATE, 
                    FEECLASS, 
                    FEECODE, 
                    FEEDESC, 
                    FEEGROUP, 
                    FEETYPEKEY, 
                    INITIALDEPFEETYPEKEY, 
                    MODBY, 
                    MODDTTM, 
                    ORDERING, 
                    PAYMENTMETHODFLAG, 
                    PAYORD, 
                    PORTALDESCRIPTION, 
                    REFUNDABLE, 
                    SRCBGTNOKEY, 
                    SURC, 
                    USEINJOBESTIMATION, 
                    VARIATION_ID, 
                    VERSIONDATEBIND, 
                    VERSIONDATEMONIKER, 
                    WAIVABLE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCTPREFIX, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ATCOSTUSEDFOR, 
                    src.AUTOPAY, 
                    src.CDRPRODUCTFAMILY, 
                    src.DELETED, 
                    src.DESTBGTNOKEY, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FEECLASS, 
                    src.FEECODE, 
                    src.FEEDESC, 
                    src.FEEGROUP, 
                    src.FEETYPEKEY, 
                    src.INITIALDEPFEETYPEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ORDERING, 
                    src.PAYMENTMETHODFLAG, 
                    src.PAYORD, 
                    src.PORTALDESCRIPTION, 
                    src.REFUNDABLE, 
                    src.SRCBGTNOKEY, 
                    src.SURC, 
                    src.USEINJOBESTIMATION, 
                    src.VARIATION_ID, 
                    src.VERSIONDATEBIND, 
                    src.VERSIONDATEMONIKER, 
                    src.WAIVABLE,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_FEETYPE as target using (
                SELECT * FROM (SELECT 
            strm.ACCTPREFIX, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ATCOSTUSEDFOR, 
            strm.AUTOPAY, 
            strm.CDRPRODUCTFAMILY, 
            strm.DELETED, 
            strm.DESTBGTNOKEY, 
            strm.DPDESC, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FEECLASS, 
            strm.FEECODE, 
            strm.FEEDESC, 
            strm.FEEGROUP, 
            strm.FEETYPEKEY, 
            strm.INITIALDEPFEETYPEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ORDERING, 
            strm.PAYMENTMETHODFLAG, 
            strm.PAYORD, 
            strm.PORTALDESCRIPTION, 
            strm.REFUNDABLE, 
            strm.SRCBGTNOKEY, 
            strm.SURC, 
            strm.USEINJOBESTIMATION, 
            strm.VARIATION_ID, 
            strm.VERSIONDATEBIND, 
            strm.VERSIONDATEMONIKER, 
            strm.WAIVABLE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FEETYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_FEETYPE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FEETYPEKEY=src.FEETYPEKEY) OR (target.FEETYPEKEY IS NULL AND src.FEETYPEKEY IS NULL)) 
                when matched then update set
                    target.ACCTPREFIX=src.ACCTPREFIX, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ATCOSTUSEDFOR=src.ATCOSTUSEDFOR, 
                    target.AUTOPAY=src.AUTOPAY, 
                    target.CDRPRODUCTFAMILY=src.CDRPRODUCTFAMILY, 
                    target.DELETED=src.DELETED, 
                    target.DESTBGTNOKEY=src.DESTBGTNOKEY, 
                    target.DPDESC=src.DPDESC, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FEECLASS=src.FEECLASS, 
                    target.FEECODE=src.FEECODE, 
                    target.FEEDESC=src.FEEDESC, 
                    target.FEEGROUP=src.FEEGROUP, 
                    target.FEETYPEKEY=src.FEETYPEKEY, 
                    target.INITIALDEPFEETYPEKEY=src.INITIALDEPFEETYPEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ORDERING=src.ORDERING, 
                    target.PAYMENTMETHODFLAG=src.PAYMENTMETHODFLAG, 
                    target.PAYORD=src.PAYORD, 
                    target.PORTALDESCRIPTION=src.PORTALDESCRIPTION, 
                    target.REFUNDABLE=src.REFUNDABLE, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.SURC=src.SURC, 
                    target.USEINJOBESTIMATION=src.USEINJOBESTIMATION, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSIONDATEBIND=src.VERSIONDATEBIND, 
                    target.VERSIONDATEMONIKER=src.VERSIONDATEMONIKER, 
                    target.WAIVABLE=src.WAIVABLE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCTPREFIX, ADDBY, ADDDTTM, ATCOSTUSEDFOR, AUTOPAY, CDRPRODUCTFAMILY, DELETED, DESTBGTNOKEY, DPDESC, EFFDATE, EXPDATE, FEECLASS, FEECODE, FEEDESC, FEEGROUP, FEETYPEKEY, INITIALDEPFEETYPEKEY, MODBY, MODDTTM, ORDERING, PAYMENTMETHODFLAG, PAYORD, PORTALDESCRIPTION, REFUNDABLE, SRCBGTNOKEY, SURC, USEINJOBESTIMATION, VARIATION_ID, VERSIONDATEBIND, VERSIONDATEMONIKER, WAIVABLE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCTPREFIX, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ATCOSTUSEDFOR, 
                    src.AUTOPAY, 
                    src.CDRPRODUCTFAMILY, 
                    src.DELETED, 
                    src.DESTBGTNOKEY, 
                    src.DPDESC, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FEECLASS, 
                    src.FEECODE, 
                    src.FEEDESC, 
                    src.FEEGROUP, 
                    src.FEETYPEKEY, 
                    src.INITIALDEPFEETYPEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ORDERING, 
                    src.PAYMENTMETHODFLAG, 
                    src.PAYORD, 
                    src.PORTALDESCRIPTION, 
                    src.REFUNDABLE, 
                    src.SRCBGTNOKEY, 
                    src.SURC, 
                    src.USEINJOBESTIMATION, 
                    src.VARIATION_ID, 
                    src.VERSIONDATEBIND, 
                    src.VERSIONDATEMONIKER, 
                    src.WAIVABLE,     
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