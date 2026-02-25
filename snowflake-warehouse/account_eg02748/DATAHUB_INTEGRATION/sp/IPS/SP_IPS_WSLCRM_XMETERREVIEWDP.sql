create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCRM_XMETERREVIEWDP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCRM_XMETERREVIEWDP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCRM_XMETERREVIEWDP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVALDATE, 
            strm.CHECKREADISCHARGEABLE, 
            strm.COMMENTS, 
            strm.DELETED, 
            strm.ISCHECKREADREQUEST, 
            strm.ISREADAPPROVED, 
            strm.ISREADNOACTION, 
            strm.ISREADREJECTED, 
            strm.MARKASANOMALY, 
            strm.MARKASNOREAD, 
            strm.MARKASREADYTOBILL, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PHOTOREQUIRED, 
            strm.REASSIGNTO, 
            strm.REJECTEDDATE, 
            strm.SERVNO, 
            strm.VARIATION_ID, 
            strm.XMETERREVIEWDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XMETERREVIEWDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XMETERREVIEWDP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XMETERREVIEWDPKEY=src.XMETERREVIEWDPKEY) OR (target.XMETERREVIEWDPKEY IS NULL AND src.XMETERREVIEWDPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVALDATE=src.APPROVALDATE, 
                    target.CHECKREADISCHARGEABLE=src.CHECKREADISCHARGEABLE, 
                    target.COMMENTS=src.COMMENTS, 
                    target.DELETED=src.DELETED, 
                    target.ISCHECKREADREQUEST=src.ISCHECKREADREQUEST, 
                    target.ISREADAPPROVED=src.ISREADAPPROVED, 
                    target.ISREADNOACTION=src.ISREADNOACTION, 
                    target.ISREADREJECTED=src.ISREADREJECTED, 
                    target.MARKASANOMALY=src.MARKASANOMALY, 
                    target.MARKASNOREAD=src.MARKASNOREAD, 
                    target.MARKASREADYTOBILL=src.MARKASREADYTOBILL, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PHOTOREQUIRED=src.PHOTOREQUIRED, 
                    target.REASSIGNTO=src.REASSIGNTO, 
                    target.REJECTEDDATE=src.REJECTEDDATE, 
                    target.SERVNO=src.SERVNO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XMETERREVIEWDPKEY=src.XMETERREVIEWDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPROVALDATE, 
                    CHECKREADISCHARGEABLE, 
                    COMMENTS, 
                    DELETED, 
                    ISCHECKREADREQUEST, 
                    ISREADAPPROVED, 
                    ISREADNOACTION, 
                    ISREADREJECTED, 
                    MARKASANOMALY, 
                    MARKASNOREAD, 
                    MARKASREADYTOBILL, 
                    MODBY, 
                    MODDTTM, 
                    PHOTOREQUIRED, 
                    REASSIGNTO, 
                    REJECTEDDATE, 
                    SERVNO, 
                    VARIATION_ID, 
                    XMETERREVIEWDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVALDATE, 
                    src.CHECKREADISCHARGEABLE, 
                    src.COMMENTS, 
                    src.DELETED, 
                    src.ISCHECKREADREQUEST, 
                    src.ISREADAPPROVED, 
                    src.ISREADNOACTION, 
                    src.ISREADREJECTED, 
                    src.MARKASANOMALY, 
                    src.MARKASNOREAD, 
                    src.MARKASREADYTOBILL, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PHOTOREQUIRED, 
                    src.REASSIGNTO, 
                    src.REJECTEDDATE, 
                    src.SERVNO, 
                    src.VARIATION_ID, 
                    src.XMETERREVIEWDPKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCRM_XMETERREVIEWDP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVALDATE, 
            strm.CHECKREADISCHARGEABLE, 
            strm.COMMENTS, 
            strm.DELETED, 
            strm.ISCHECKREADREQUEST, 
            strm.ISREADAPPROVED, 
            strm.ISREADNOACTION, 
            strm.ISREADREJECTED, 
            strm.MARKASANOMALY, 
            strm.MARKASNOREAD, 
            strm.MARKASREADYTOBILL, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PHOTOREQUIRED, 
            strm.REASSIGNTO, 
            strm.REJECTEDDATE, 
            strm.SERVNO, 
            strm.VARIATION_ID, 
            strm.XMETERREVIEWDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XMETERREVIEWDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XMETERREVIEWDP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XMETERREVIEWDPKEY=src.XMETERREVIEWDPKEY) OR (target.XMETERREVIEWDPKEY IS NULL AND src.XMETERREVIEWDPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVALDATE=src.APPROVALDATE, 
                    target.CHECKREADISCHARGEABLE=src.CHECKREADISCHARGEABLE, 
                    target.COMMENTS=src.COMMENTS, 
                    target.DELETED=src.DELETED, 
                    target.ISCHECKREADREQUEST=src.ISCHECKREADREQUEST, 
                    target.ISREADAPPROVED=src.ISREADAPPROVED, 
                    target.ISREADNOACTION=src.ISREADNOACTION, 
                    target.ISREADREJECTED=src.ISREADREJECTED, 
                    target.MARKASANOMALY=src.MARKASANOMALY, 
                    target.MARKASNOREAD=src.MARKASNOREAD, 
                    target.MARKASREADYTOBILL=src.MARKASREADYTOBILL, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PHOTOREQUIRED=src.PHOTOREQUIRED, 
                    target.REASSIGNTO=src.REASSIGNTO, 
                    target.REJECTEDDATE=src.REJECTEDDATE, 
                    target.SERVNO=src.SERVNO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XMETERREVIEWDPKEY=src.XMETERREVIEWDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPROVALDATE, CHECKREADISCHARGEABLE, COMMENTS, DELETED, ISCHECKREADREQUEST, ISREADAPPROVED, ISREADNOACTION, ISREADREJECTED, MARKASANOMALY, MARKASNOREAD, MARKASREADYTOBILL, MODBY, MODDTTM, PHOTOREQUIRED, REASSIGNTO, REJECTEDDATE, SERVNO, VARIATION_ID, XMETERREVIEWDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVALDATE, 
                    src.CHECKREADISCHARGEABLE, 
                    src.COMMENTS, 
                    src.DELETED, 
                    src.ISCHECKREADREQUEST, 
                    src.ISREADAPPROVED, 
                    src.ISREADNOACTION, 
                    src.ISREADREJECTED, 
                    src.MARKASANOMALY, 
                    src.MARKASNOREAD, 
                    src.MARKASREADYTOBILL, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PHOTOREQUIRED, 
                    src.REASSIGNTO, 
                    src.REJECTEDDATE, 
                    src.SERVNO, 
                    src.VARIATION_ID, 
                    src.XMETERREVIEWDPKEY,     
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