create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_APREFUNDTRN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_APREFUNDTRN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_APREFUNDTRN as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMTCONF, 
            strm.APKEY, 
            strm.APRFNDTRNNO, 
            strm.CARDAUTH, 
            strm.CARDBANK, 
            strm.CARDEXPDT, 
            strm.CARDNAME, 
            strm.CARDNO, 
            strm.CARDTYPE, 
            strm.CHECKBANK, 
            strm.CHECKNO, 
            strm.CNTCTKEY, 
            strm.COMMENTS, 
            strm.CONFDTTM, 
            strm.DELETED, 
            strm.ESCROWACCTKEY, 
            strm.ESCROWNO, 
            strm.ESCROWTRNNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYMENTMETHOD, 
            strm.PAYMETHOD, 
            strm.REFUNDAMT, 
            strm.REGTRANNO, 
            strm.SRCBGTNOKEY, 
            strm.STAT, 
            strm.TRNDTTM, 
            strm.TRNEMP, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APRFNDTRNNO ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APREFUNDTRN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APRFNDTRNNO=src.APRFNDTRNNO) OR (target.APRFNDTRNNO IS NULL AND src.APRFNDTRNNO IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMTCONF=src.AMTCONF, 
                    target.APKEY=src.APKEY, 
                    target.APRFNDTRNNO=src.APRFNDTRNNO, 
                    target.CARDAUTH=src.CARDAUTH, 
                    target.CARDBANK=src.CARDBANK, 
                    target.CARDEXPDT=src.CARDEXPDT, 
                    target.CARDNAME=src.CARDNAME, 
                    target.CARDNO=src.CARDNO, 
                    target.CARDTYPE=src.CARDTYPE, 
                    target.CHECKBANK=src.CHECKBANK, 
                    target.CHECKNO=src.CHECKNO, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.CONFDTTM=src.CONFDTTM, 
                    target.DELETED=src.DELETED, 
                    target.ESCROWACCTKEY=src.ESCROWACCTKEY, 
                    target.ESCROWNO=src.ESCROWNO, 
                    target.ESCROWTRNNO=src.ESCROWTRNNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYMENTMETHOD=src.PAYMENTMETHOD, 
                    target.PAYMETHOD=src.PAYMETHOD, 
                    target.REFUNDAMT=src.REFUNDAMT, 
                    target.REGTRANNO=src.REGTRANNO, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.STAT=src.STAT, 
                    target.TRNDTTM=src.TRNDTTM, 
                    target.TRNEMP=src.TRNEMP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AMTCONF, 
                    APKEY, 
                    APRFNDTRNNO, 
                    CARDAUTH, 
                    CARDBANK, 
                    CARDEXPDT, 
                    CARDNAME, 
                    CARDNO, 
                    CARDTYPE, 
                    CHECKBANK, 
                    CHECKNO, 
                    CNTCTKEY, 
                    COMMENTS, 
                    CONFDTTM, 
                    DELETED, 
                    ESCROWACCTKEY, 
                    ESCROWNO, 
                    ESCROWTRNNO, 
                    MODBY, 
                    MODDTTM, 
                    PAYMENTMETHOD, 
                    PAYMETHOD, 
                    REFUNDAMT, 
                    REGTRANNO, 
                    SRCBGTNOKEY, 
                    STAT, 
                    TRNDTTM, 
                    TRNEMP, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMTCONF, 
                    src.APKEY, 
                    src.APRFNDTRNNO, 
                    src.CARDAUTH, 
                    src.CARDBANK, 
                    src.CARDEXPDT, 
                    src.CARDNAME, 
                    src.CARDNO, 
                    src.CARDTYPE, 
                    src.CHECKBANK, 
                    src.CHECKNO, 
                    src.CNTCTKEY, 
                    src.COMMENTS, 
                    src.CONFDTTM, 
                    src.DELETED, 
                    src.ESCROWACCTKEY, 
                    src.ESCROWNO, 
                    src.ESCROWTRNNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYMENTMETHOD, 
                    src.PAYMETHOD, 
                    src.REFUNDAMT, 
                    src.REGTRANNO, 
                    src.SRCBGTNOKEY, 
                    src.STAT, 
                    src.TRNDTTM, 
                    src.TRNEMP, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_APREFUNDTRN as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AMTCONF, 
            strm.APKEY, 
            strm.APRFNDTRNNO, 
            strm.CARDAUTH, 
            strm.CARDBANK, 
            strm.CARDEXPDT, 
            strm.CARDNAME, 
            strm.CARDNO, 
            strm.CARDTYPE, 
            strm.CHECKBANK, 
            strm.CHECKNO, 
            strm.CNTCTKEY, 
            strm.COMMENTS, 
            strm.CONFDTTM, 
            strm.DELETED, 
            strm.ESCROWACCTKEY, 
            strm.ESCROWNO, 
            strm.ESCROWTRNNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYMENTMETHOD, 
            strm.PAYMETHOD, 
            strm.REFUNDAMT, 
            strm.REGTRANNO, 
            strm.SRCBGTNOKEY, 
            strm.STAT, 
            strm.TRNDTTM, 
            strm.TRNEMP, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APRFNDTRNNO ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APREFUNDTRN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APRFNDTRNNO=src.APRFNDTRNNO) OR (target.APRFNDTRNNO IS NULL AND src.APRFNDTRNNO IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AMTCONF=src.AMTCONF, 
                    target.APKEY=src.APKEY, 
                    target.APRFNDTRNNO=src.APRFNDTRNNO, 
                    target.CARDAUTH=src.CARDAUTH, 
                    target.CARDBANK=src.CARDBANK, 
                    target.CARDEXPDT=src.CARDEXPDT, 
                    target.CARDNAME=src.CARDNAME, 
                    target.CARDNO=src.CARDNO, 
                    target.CARDTYPE=src.CARDTYPE, 
                    target.CHECKBANK=src.CHECKBANK, 
                    target.CHECKNO=src.CHECKNO, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.CONFDTTM=src.CONFDTTM, 
                    target.DELETED=src.DELETED, 
                    target.ESCROWACCTKEY=src.ESCROWACCTKEY, 
                    target.ESCROWNO=src.ESCROWNO, 
                    target.ESCROWTRNNO=src.ESCROWTRNNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYMENTMETHOD=src.PAYMENTMETHOD, 
                    target.PAYMETHOD=src.PAYMETHOD, 
                    target.REFUNDAMT=src.REFUNDAMT, 
                    target.REGTRANNO=src.REGTRANNO, 
                    target.SRCBGTNOKEY=src.SRCBGTNOKEY, 
                    target.STAT=src.STAT, 
                    target.TRNDTTM=src.TRNDTTM, 
                    target.TRNEMP=src.TRNEMP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AMTCONF, APKEY, APRFNDTRNNO, CARDAUTH, CARDBANK, CARDEXPDT, CARDNAME, CARDNO, CARDTYPE, CHECKBANK, CHECKNO, CNTCTKEY, COMMENTS, CONFDTTM, DELETED, ESCROWACCTKEY, ESCROWNO, ESCROWTRNNO, MODBY, MODDTTM, PAYMENTMETHOD, PAYMETHOD, REFUNDAMT, REGTRANNO, SRCBGTNOKEY, STAT, TRNDTTM, TRNEMP, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AMTCONF, 
                    src.APKEY, 
                    src.APRFNDTRNNO, 
                    src.CARDAUTH, 
                    src.CARDBANK, 
                    src.CARDEXPDT, 
                    src.CARDNAME, 
                    src.CARDNO, 
                    src.CARDTYPE, 
                    src.CHECKBANK, 
                    src.CHECKNO, 
                    src.CNTCTKEY, 
                    src.COMMENTS, 
                    src.CONFDTTM, 
                    src.DELETED, 
                    src.ESCROWACCTKEY, 
                    src.ESCROWNO, 
                    src.ESCROWTRNNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYMENTMETHOD, 
                    src.PAYMETHOD, 
                    src.REFUNDAMT, 
                    src.REGTRANNO, 
                    src.SRCBGTNOKEY, 
                    src.STAT, 
                    src.TRNDTTM, 
                    src.TRNEMP, 
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