create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS060()
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
                            INSERT INTO LANDING_ERROR.LN_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS060_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS060 as target using (SELECT * FROM (SELECT 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccty, 
            strm.ccty_ref_compnr, 
            strm.cdis, 
            strm.cdis_ref_compnr, 
            strm.cmnf, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.exdt, 
            strm.indt, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cmnf ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS060 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cmnf=src.cmnf) OR (target.cmnf IS NULL AND src.cmnf IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccty=src.ccty, 
                    target.ccty_ref_compnr=src.ccty_ref_compnr, 
                    target.cdis=src.cdis, 
                    target.cdis_ref_compnr=src.cdis_ref_compnr, 
                    target.cmnf=src.cmnf, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.exdt=src.exdt, 
                    target.indt=src.indt, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cadr, 
                    cadr_ref_compnr, 
                    ccty, 
                    ccty_ref_compnr, 
                    cdis, 
                    cdis_ref_compnr, 
                    cmnf, 
                    compnr, 
                    deleted, 
                    dsca, 
                    exdt, 
                    indt, 
                    otbp, 
                    otbp_ref_compnr, 
                    sequencenumber, 
                    stat, 
                    stat_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccty, 
                    src.ccty_ref_compnr, 
                    src.cdis, 
                    src.cdis_ref_compnr, 
                    src.cmnf, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.exdt, 
                    src.indt, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS060_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccty, 
            strm.ccty_ref_compnr, 
            strm.cdis, 
            strm.cdis_ref_compnr, 
            strm.cmnf, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.exdt, 
            strm.indt, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cmnf ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS060 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cmnf=src.cmnf) OR (target.cmnf IS NULL AND src.cmnf IS NULL)) 
                when matched then update set
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccty=src.ccty, 
                    target.ccty_ref_compnr=src.ccty_ref_compnr, 
                    target.cdis=src.cdis, 
                    target.cdis_ref_compnr=src.cdis_ref_compnr, 
                    target.cmnf=src.cmnf, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.exdt=src.exdt, 
                    target.indt=src.indt, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cadr, cadr_ref_compnr, ccty, ccty_ref_compnr, cdis, cdis_ref_compnr, cmnf, compnr, deleted, dsca, exdt, indt, otbp, otbp_ref_compnr, sequencenumber, stat, stat_kw, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccty, 
                    src.ccty_ref_compnr, 
                    src.cdis, 
                    src.cdis_ref_compnr, 
                    src.cmnf, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.exdt, 
                    src.indt, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
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