create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD007()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD007_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD007 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.dacp, 
            strm.dacr, 
            strm.dcmg, 
            strm.deleted, 
            strm.dgld, 
            strm.dint, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.ptyp_year_prno_ref_compnr, 
            strm.sacp, 
            strm.sacp_kw, 
            strm.sacr, 
            strm.sacr_kw, 
            strm.scmg, 
            strm.scmg_kw, 
            strm.sequencenumber, 
            strm.sgld, 
            strm.sgld_kw, 
            strm.sint, 
            strm.sint_kw, 
            strm.timestamp, 
            strm.uacp, 
            strm.uacr, 
            strm.ucmg, 
            strm.ugld, 
            strm.uint, 
            strm.username, 
            strm.year, 
            strm.year_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ptyp,
            strm.year,
            strm.prno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD007 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.dacp=src.dacp, 
                    target.dacr=src.dacr, 
                    target.dcmg=src.dcmg, 
                    target.deleted=src.deleted, 
                    target.dgld=src.dgld, 
                    target.dint=src.dint, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.ptyp_year_prno_ref_compnr=src.ptyp_year_prno_ref_compnr, 
                    target.sacp=src.sacp, 
                    target.sacp_kw=src.sacp_kw, 
                    target.sacr=src.sacr, 
                    target.sacr_kw=src.sacr_kw, 
                    target.scmg=src.scmg, 
                    target.scmg_kw=src.scmg_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sgld=src.sgld, 
                    target.sgld_kw=src.sgld_kw, 
                    target.sint=src.sint, 
                    target.sint_kw=src.sint_kw, 
                    target.timestamp=src.timestamp, 
                    target.uacp=src.uacp, 
                    target.uacr=src.uacr, 
                    target.ucmg=src.ucmg, 
                    target.ugld=src.ugld, 
                    target.uint=src.uint, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.year_ref_compnr=src.year_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    dacp, 
                    dacr, 
                    dcmg, 
                    deleted, 
                    dgld, 
                    dint, 
                    prno, 
                    ptyp, 
                    ptyp_kw, 
                    ptyp_year_prno_ref_compnr, 
                    sacp, 
                    sacp_kw, 
                    sacr, 
                    sacr_kw, 
                    scmg, 
                    scmg_kw, 
                    sequencenumber, 
                    sgld, 
                    sgld_kw, 
                    sint, 
                    sint_kw, 
                    timestamp, 
                    uacp, 
                    uacr, 
                    ucmg, 
                    ugld, 
                    uint, 
                    username, 
                    year, 
                    year_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.dacp, 
                    src.dacr, 
                    src.dcmg, 
                    src.deleted, 
                    src.dgld, 
                    src.dint, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.ptyp_year_prno_ref_compnr, 
                    src.sacp, 
                    src.sacp_kw, 
                    src.sacr, 
                    src.sacr_kw, 
                    src.scmg, 
                    src.scmg_kw, 
                    src.sequencenumber, 
                    src.sgld, 
                    src.sgld_kw, 
                    src.sint, 
                    src.sint_kw, 
                    src.timestamp, 
                    src.uacp, 
                    src.uacr, 
                    src.ucmg, 
                    src.ugld, 
                    src.uint, 
                    src.username, 
                    src.year, 
                    src.year_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD007_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.dacp, 
            strm.dacr, 
            strm.dcmg, 
            strm.deleted, 
            strm.dgld, 
            strm.dint, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.ptyp_year_prno_ref_compnr, 
            strm.sacp, 
            strm.sacp_kw, 
            strm.sacr, 
            strm.sacr_kw, 
            strm.scmg, 
            strm.scmg_kw, 
            strm.sequencenumber, 
            strm.sgld, 
            strm.sgld_kw, 
            strm.sint, 
            strm.sint_kw, 
            strm.timestamp, 
            strm.uacp, 
            strm.uacr, 
            strm.ucmg, 
            strm.ugld, 
            strm.uint, 
            strm.username, 
            strm.year, 
            strm.year_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ptyp,
            strm.year,
            strm.prno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD007 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.dacp=src.dacp, 
                    target.dacr=src.dacr, 
                    target.dcmg=src.dcmg, 
                    target.deleted=src.deleted, 
                    target.dgld=src.dgld, 
                    target.dint=src.dint, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.ptyp_year_prno_ref_compnr=src.ptyp_year_prno_ref_compnr, 
                    target.sacp=src.sacp, 
                    target.sacp_kw=src.sacp_kw, 
                    target.sacr=src.sacr, 
                    target.sacr_kw=src.sacr_kw, 
                    target.scmg=src.scmg, 
                    target.scmg_kw=src.scmg_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sgld=src.sgld, 
                    target.sgld_kw=src.sgld_kw, 
                    target.sint=src.sint, 
                    target.sint_kw=src.sint_kw, 
                    target.timestamp=src.timestamp, 
                    target.uacp=src.uacp, 
                    target.uacr=src.uacr, 
                    target.ucmg=src.ucmg, 
                    target.ugld=src.ugld, 
                    target.uint=src.uint, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.year_ref_compnr=src.year_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, dacp, dacr, dcmg, deleted, dgld, dint, prno, ptyp, ptyp_kw, ptyp_year_prno_ref_compnr, sacp, sacp_kw, sacr, sacr_kw, scmg, scmg_kw, sequencenumber, sgld, sgld_kw, sint, sint_kw, timestamp, uacp, uacr, ucmg, ugld, uint, username, year, year_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.dacp, 
                    src.dacr, 
                    src.dcmg, 
                    src.deleted, 
                    src.dgld, 
                    src.dint, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.ptyp_year_prno_ref_compnr, 
                    src.sacp, 
                    src.sacp_kw, 
                    src.sacr, 
                    src.sacr_kw, 
                    src.scmg, 
                    src.scmg_kw, 
                    src.sequencenumber, 
                    src.sgld, 
                    src.sgld_kw, 
                    src.sint, 
                    src.sint_kw, 
                    src.timestamp, 
                    src.uacp, 
                    src.uacr, 
                    src.ucmg, 
                    src.ugld, 
                    src.uint, 
                    src.username, 
                    src.year, 
                    src.year_ref_compnr,     
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