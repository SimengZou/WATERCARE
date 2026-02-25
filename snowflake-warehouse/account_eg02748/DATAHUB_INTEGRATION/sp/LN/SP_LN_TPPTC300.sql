create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC300()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC300_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC300 as target using (SELECT * FROM (SELECT 
            strm.actl, 
            strm.actl_kw, 
            strm.bdtp, 
            strm.bdtp_kw, 
            strm.ccal, 
            strm.cexc, 
            strm.cexc_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.crdt, 
            strm.defn, 
            strm.defn_kw, 
            strm.deleted, 
            strm.desc, 
            strm.exdt, 
            strm.free, 
            strm.free_kw, 
            strm.ibud, 
            strm.ibud_kw, 
            strm.icon, 
            strm.icon_kw, 
            strm.iexc, 
            strm.iexc_kw, 
            strm.lcdt, 
            strm.lclg, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.upmd, 
            strm.upmd_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.ccal ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC300 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.actl=src.actl, 
                    target.actl_kw=src.actl_kw, 
                    target.bdtp=src.bdtp, 
                    target.bdtp_kw=src.bdtp_kw, 
                    target.ccal=src.ccal, 
                    target.cexc=src.cexc, 
                    target.cexc_ref_compnr=src.cexc_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.crdt=src.crdt, 
                    target.defn=src.defn, 
                    target.defn_kw=src.defn_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.exdt=src.exdt, 
                    target.free=src.free, 
                    target.free_kw=src.free_kw, 
                    target.ibud=src.ibud, 
                    target.ibud_kw=src.ibud_kw, 
                    target.icon=src.icon, 
                    target.icon_kw=src.icon_kw, 
                    target.iexc=src.iexc, 
                    target.iexc_kw=src.iexc_kw, 
                    target.lcdt=src.lcdt, 
                    target.lclg=src.lclg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.upmd=src.upmd, 
                    target.upmd_kw=src.upmd_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    actl, 
                    actl_kw, 
                    bdtp, 
                    bdtp_kw, 
                    ccal, 
                    cexc, 
                    cexc_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_ref_compnr, 
                    cprj_ref_compnr, 
                    crdt, 
                    defn, 
                    defn_kw, 
                    deleted, 
                    desc, 
                    exdt, 
                    free, 
                    free_kw, 
                    ibud, 
                    ibud_kw, 
                    icon, 
                    icon_kw, 
                    iexc, 
                    iexc_kw, 
                    lcdt, 
                    lclg, 
                    sequencenumber, 
                    timestamp, 
                    upmd, 
                    upmd_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.actl, 
                    src.actl_kw, 
                    src.bdtp, 
                    src.bdtp_kw, 
                    src.ccal, 
                    src.cexc, 
                    src.cexc_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.crdt, 
                    src.defn, 
                    src.defn_kw, 
                    src.deleted, 
                    src.desc, 
                    src.exdt, 
                    src.free, 
                    src.free_kw, 
                    src.ibud, 
                    src.ibud_kw, 
                    src.icon, 
                    src.icon_kw, 
                    src.iexc, 
                    src.iexc_kw, 
                    src.lcdt, 
                    src.lclg, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.upmd, 
                    src.upmd_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC300_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.actl, 
            strm.actl_kw, 
            strm.bdtp, 
            strm.bdtp_kw, 
            strm.ccal, 
            strm.cexc, 
            strm.cexc_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.crdt, 
            strm.defn, 
            strm.defn_kw, 
            strm.deleted, 
            strm.desc, 
            strm.exdt, 
            strm.free, 
            strm.free_kw, 
            strm.ibud, 
            strm.ibud_kw, 
            strm.icon, 
            strm.icon_kw, 
            strm.iexc, 
            strm.iexc_kw, 
            strm.lcdt, 
            strm.lclg, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.upmd, 
            strm.upmd_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.ccal ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC300 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) 
                when matched then update set
                    target.actl=src.actl, 
                    target.actl_kw=src.actl_kw, 
                    target.bdtp=src.bdtp, 
                    target.bdtp_kw=src.bdtp_kw, 
                    target.ccal=src.ccal, 
                    target.cexc=src.cexc, 
                    target.cexc_ref_compnr=src.cexc_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.crdt=src.crdt, 
                    target.defn=src.defn, 
                    target.defn_kw=src.defn_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.exdt=src.exdt, 
                    target.free=src.free, 
                    target.free_kw=src.free_kw, 
                    target.ibud=src.ibud, 
                    target.ibud_kw=src.ibud_kw, 
                    target.icon=src.icon, 
                    target.icon_kw=src.icon_kw, 
                    target.iexc=src.iexc, 
                    target.iexc_kw=src.iexc_kw, 
                    target.lcdt=src.lcdt, 
                    target.lclg=src.lclg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.upmd=src.upmd, 
                    target.upmd_kw=src.upmd_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( actl, actl_kw, bdtp, bdtp_kw, ccal, cexc, cexc_ref_compnr, compnr, cpla, cprj, cprj_cpla_ref_compnr, cprj_ref_compnr, crdt, defn, defn_kw, deleted, desc, exdt, free, free_kw, ibud, ibud_kw, icon, icon_kw, iexc, iexc_kw, lcdt, lclg, sequencenumber, timestamp, upmd, upmd_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.actl, 
                    src.actl_kw, 
                    src.bdtp, 
                    src.bdtp_kw, 
                    src.ccal, 
                    src.cexc, 
                    src.cexc_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.crdt, 
                    src.defn, 
                    src.defn_kw, 
                    src.deleted, 
                    src.desc, 
                    src.exdt, 
                    src.free, 
                    src.free_kw, 
                    src.ibud, 
                    src.ibud_kw, 
                    src.icon, 
                    src.icon_kw, 
                    src.iexc, 
                    src.iexc_kw, 
                    src.lcdt, 
                    src.lclg, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.upmd, 
                    src.upmd_kw, 
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