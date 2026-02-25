create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLINTERFACE_DOCUMENTDETAILS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLINTERFACE_DOCUMENTDETAILS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLINTERFACE_DOCUMENTDETAILS as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVALDATE, 
            strm.CATEGORY, 
            strm.CT, 
            strm.CUSTOMERNAME, 
            strm.DATALAKE_DELETED, 
            strm.DOCUMENTDETAILSKEY, 
            strm.DOCUMENTTYPE, 
            strm.DP, 
            strm.EMAILKEYCOLUMN, 
            strm.ENGINEERFULLNAME, 
            strm.EPANUMBER, 
            strm.FROMEMAIL, 
            strm.JOBNAME, 
            strm.KEYCOLUMN, 
            strm.LOOKUPMONIKER, 
            strm.LOOKUPSOURCE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONIKER, 
            strm.NAME, 
            strm.POSTCODE, 
            strm.REVIEWERDDI, 
            strm.REVIEWEREMAIL, 
            strm.REVIEWERNAME, 
            strm.SOURCE, 
            strm.STAGES, 
            strm.STREETNAME, 
            strm.STREETNUMBER, 
            strm.SUBJECT, 
            strm.SUBURB, 
            strm.TOEMAIL, 
            strm.VARIATION_ID, 
            strm.VESTEDAMOUNT, 
            strm.WASTEWATERCONNECTION, 
            strm.WATERCAREREFNO, 
            strm.WATERCONNECTION, 
            strm.WORKOVERAPPLICATIONNO, 
            strm.WORKSOVER, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DOCUMENTDETAILSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_DOCUMENTDETAILS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DOCUMENTDETAILSKEY=src.DOCUMENTDETAILSKEY) OR (target.DOCUMENTDETAILSKEY IS NULL AND src.DOCUMENTDETAILSKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVALDATE=src.APPROVALDATE, 
                    target.CATEGORY=src.CATEGORY, 
                    target.CT=src.CT, 
                    target.CUSTOMERNAME=src.CUSTOMERNAME, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DOCUMENTDETAILSKEY=src.DOCUMENTDETAILSKEY, 
                    target.DOCUMENTTYPE=src.DOCUMENTTYPE, 
                    target.DP=src.DP, 
                    target.EMAILKEYCOLUMN=src.EMAILKEYCOLUMN, 
                    target.ENGINEERFULLNAME=src.ENGINEERFULLNAME, 
                    target.EPANUMBER=src.EPANUMBER, 
                    target.FROMEMAIL=src.FROMEMAIL, 
                    target.JOBNAME=src.JOBNAME, 
                    target.KEYCOLUMN=src.KEYCOLUMN, 
                    target.LOOKUPMONIKER=src.LOOKUPMONIKER, 
                    target.LOOKUPSOURCE=src.LOOKUPSOURCE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONIKER=src.MONIKER, 
                    target.NAME=src.NAME, 
                    target.POSTCODE=src.POSTCODE, 
                    target.REVIEWERDDI=src.REVIEWERDDI, 
                    target.REVIEWEREMAIL=src.REVIEWEREMAIL, 
                    target.REVIEWERNAME=src.REVIEWERNAME, 
                    target.SOURCE=src.SOURCE, 
                    target.STAGES=src.STAGES, 
                    target.STREETNAME=src.STREETNAME, 
                    target.STREETNUMBER=src.STREETNUMBER, 
                    target.SUBJECT=src.SUBJECT, 
                    target.SUBURB=src.SUBURB, 
                    target.TOEMAIL=src.TOEMAIL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VESTEDAMOUNT=src.VESTEDAMOUNT, 
                    target.WASTEWATERCONNECTION=src.WASTEWATERCONNECTION, 
                    target.WATERCAREREFNO=src.WATERCAREREFNO, 
                    target.WATERCONNECTION=src.WATERCONNECTION, 
                    target.WORKOVERAPPLICATIONNO=src.WORKOVERAPPLICATIONNO, 
                    target.WORKSOVER=src.WORKSOVER, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPROVALDATE, 
                    CATEGORY, 
                    CT, 
                    CUSTOMERNAME, 
                    DATALAKE_DELETED, 
                    DOCUMENTDETAILSKEY, 
                    DOCUMENTTYPE, 
                    DP, 
                    EMAILKEYCOLUMN, 
                    ENGINEERFULLNAME, 
                    EPANUMBER, 
                    FROMEMAIL, 
                    JOBNAME, 
                    KEYCOLUMN, 
                    LOOKUPMONIKER, 
                    LOOKUPSOURCE, 
                    LOT, 
                    MODBY, 
                    MODDTTM, 
                    MONIKER, 
                    NAME, 
                    POSTCODE, 
                    REVIEWERDDI, 
                    REVIEWEREMAIL, 
                    REVIEWERNAME, 
                    SOURCE, 
                    STAGES, 
                    STREETNAME, 
                    STREETNUMBER, 
                    SUBJECT, 
                    SUBURB, 
                    TOEMAIL, 
                    VARIATION_ID, 
                    VESTEDAMOUNT, 
                    WASTEWATERCONNECTION, 
                    WATERCAREREFNO, 
                    WATERCONNECTION, 
                    WORKOVERAPPLICATIONNO, 
                    WORKSOVER, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVALDATE, 
                    src.CATEGORY, 
                    src.CT, 
                    src.CUSTOMERNAME, 
                    src.DATALAKE_DELETED, 
                    src.DOCUMENTDETAILSKEY, 
                    src.DOCUMENTTYPE, 
                    src.DP, 
                    src.EMAILKEYCOLUMN, 
                    src.ENGINEERFULLNAME, 
                    src.EPANUMBER, 
                    src.FROMEMAIL, 
                    src.JOBNAME, 
                    src.KEYCOLUMN, 
                    src.LOOKUPMONIKER, 
                    src.LOOKUPSOURCE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONIKER, 
                    src.NAME, 
                    src.POSTCODE, 
                    src.REVIEWERDDI, 
                    src.REVIEWEREMAIL, 
                    src.REVIEWERNAME, 
                    src.SOURCE, 
                    src.STAGES, 
                    src.STREETNAME, 
                    src.STREETNUMBER, 
                    src.SUBJECT, 
                    src.SUBURB, 
                    src.TOEMAIL, 
                    src.VARIATION_ID, 
                    src.VESTEDAMOUNT, 
                    src.WASTEWATERCONNECTION, 
                    src.WATERCAREREFNO, 
                    src.WATERCONNECTION, 
                    src.WORKOVERAPPLICATIONNO, 
                    src.WORKSOVER,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_DOCUMENTDETAILS as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVALDATE, 
            strm.CATEGORY, 
            strm.CT, 
            strm.CUSTOMERNAME, 
            strm.DATALAKE_DELETED, 
            strm.DOCUMENTDETAILSKEY, 
            strm.DOCUMENTTYPE, 
            strm.DP, 
            strm.EMAILKEYCOLUMN, 
            strm.ENGINEERFULLNAME, 
            strm.EPANUMBER, 
            strm.FROMEMAIL, 
            strm.JOBNAME, 
            strm.KEYCOLUMN, 
            strm.LOOKUPMONIKER, 
            strm.LOOKUPSOURCE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONIKER, 
            strm.NAME, 
            strm.POSTCODE, 
            strm.REVIEWERDDI, 
            strm.REVIEWEREMAIL, 
            strm.REVIEWERNAME, 
            strm.SOURCE, 
            strm.STAGES, 
            strm.STREETNAME, 
            strm.STREETNUMBER, 
            strm.SUBJECT, 
            strm.SUBURB, 
            strm.TOEMAIL, 
            strm.VARIATION_ID, 
            strm.VESTEDAMOUNT, 
            strm.WASTEWATERCONNECTION, 
            strm.WATERCAREREFNO, 
            strm.WATERCONNECTION, 
            strm.WORKOVERAPPLICATIONNO, 
            strm.WORKSOVER, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DOCUMENTDETAILSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_DOCUMENTDETAILS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DOCUMENTDETAILSKEY=src.DOCUMENTDETAILSKEY) OR (target.DOCUMENTDETAILSKEY IS NULL AND src.DOCUMENTDETAILSKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVALDATE=src.APPROVALDATE, 
                    target.CATEGORY=src.CATEGORY, 
                    target.CT=src.CT, 
                    target.CUSTOMERNAME=src.CUSTOMERNAME, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DOCUMENTDETAILSKEY=src.DOCUMENTDETAILSKEY, 
                    target.DOCUMENTTYPE=src.DOCUMENTTYPE, 
                    target.DP=src.DP, 
                    target.EMAILKEYCOLUMN=src.EMAILKEYCOLUMN, 
                    target.ENGINEERFULLNAME=src.ENGINEERFULLNAME, 
                    target.EPANUMBER=src.EPANUMBER, 
                    target.FROMEMAIL=src.FROMEMAIL, 
                    target.JOBNAME=src.JOBNAME, 
                    target.KEYCOLUMN=src.KEYCOLUMN, 
                    target.LOOKUPMONIKER=src.LOOKUPMONIKER, 
                    target.LOOKUPSOURCE=src.LOOKUPSOURCE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONIKER=src.MONIKER, 
                    target.NAME=src.NAME, 
                    target.POSTCODE=src.POSTCODE, 
                    target.REVIEWERDDI=src.REVIEWERDDI, 
                    target.REVIEWEREMAIL=src.REVIEWEREMAIL, 
                    target.REVIEWERNAME=src.REVIEWERNAME, 
                    target.SOURCE=src.SOURCE, 
                    target.STAGES=src.STAGES, 
                    target.STREETNAME=src.STREETNAME, 
                    target.STREETNUMBER=src.STREETNUMBER, 
                    target.SUBJECT=src.SUBJECT, 
                    target.SUBURB=src.SUBURB, 
                    target.TOEMAIL=src.TOEMAIL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VESTEDAMOUNT=src.VESTEDAMOUNT, 
                    target.WASTEWATERCONNECTION=src.WASTEWATERCONNECTION, 
                    target.WATERCAREREFNO=src.WATERCAREREFNO, 
                    target.WATERCONNECTION=src.WATERCONNECTION, 
                    target.WORKOVERAPPLICATIONNO=src.WORKOVERAPPLICATIONNO, 
                    target.WORKSOVER=src.WORKSOVER, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPROVALDATE, CATEGORY, CT, CUSTOMERNAME, DATALAKE_DELETED, DOCUMENTDETAILSKEY, DOCUMENTTYPE, DP, EMAILKEYCOLUMN, ENGINEERFULLNAME, EPANUMBER, FROMEMAIL, JOBNAME, KEYCOLUMN, LOOKUPMONIKER, LOOKUPSOURCE, LOT, MODBY, MODDTTM, MONIKER, NAME, POSTCODE, REVIEWERDDI, REVIEWEREMAIL, REVIEWERNAME, SOURCE, STAGES, STREETNAME, STREETNUMBER, SUBJECT, SUBURB, TOEMAIL, VARIATION_ID, VESTEDAMOUNT, WASTEWATERCONNECTION, WATERCAREREFNO, WATERCONNECTION, WORKOVERAPPLICATIONNO, WORKSOVER, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVALDATE, 
                    src.CATEGORY, 
                    src.CT, 
                    src.CUSTOMERNAME, 
                    src.DATALAKE_DELETED, 
                    src.DOCUMENTDETAILSKEY, 
                    src.DOCUMENTTYPE, 
                    src.DP, 
                    src.EMAILKEYCOLUMN, 
                    src.ENGINEERFULLNAME, 
                    src.EPANUMBER, 
                    src.FROMEMAIL, 
                    src.JOBNAME, 
                    src.KEYCOLUMN, 
                    src.LOOKUPMONIKER, 
                    src.LOOKUPSOURCE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONIKER, 
                    src.NAME, 
                    src.POSTCODE, 
                    src.REVIEWERDDI, 
                    src.REVIEWEREMAIL, 
                    src.REVIEWERNAME, 
                    src.SOURCE, 
                    src.STAGES, 
                    src.STREETNAME, 
                    src.STREETNUMBER, 
                    src.SUBJECT, 
                    src.SUBURB, 
                    src.TOEMAIL, 
                    src.VARIATION_ID, 
                    src.VESTEDAMOUNT, 
                    src.WASTEWATERCONNECTION, 
                    src.WATERCAREREFNO, 
                    src.WATERCONNECTION, 
                    src.WORKOVERAPPLICATIONNO, 
                    src.WORKSOVER,     
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