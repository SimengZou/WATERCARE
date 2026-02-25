create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINH310()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINH310_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINH310 as target using (SELECT * FROM (SELECT 
            strm.carr, 
            strm.carr_ref_compnr, 
            strm.compnr, 
            strm.conf, 
            strm.conf_kw, 
            strm.crdt, 
            strm.curr, 
            strm.curr_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.cwun, 
            strm.cwun_ref_compnr, 
            strm.deleted, 
            strm.dino, 
            strm.fval, 
            strm.huid, 
            strm.huid_ref_compnr, 
            strm.imre, 
            strm.imrk, 
            strm.irdt, 
            strm.load, 
            strm.pddt, 
            strm.port, 
            strm.port_ref_compnr, 
            strm.rcno, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.shda, 
            strm.shda_ref_compnr, 
            strm.shdt, 
            strm.shid, 
            strm.stat, 
            strm.stat_kw, 
            strm.tcap, 
            strm.tcap_kw, 
            strm.tccp, 
            strm.tccp_kw, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.trcd, 
            strm.username, 
            strm.wght, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.rcno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH310 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.carr=src.carr, 
                    target.carr_ref_compnr=src.carr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.conf=src.conf, 
                    target.conf_kw=src.conf_kw, 
                    target.crdt=src.crdt, 
                    target.curr=src.curr, 
                    target.curr_ref_compnr=src.curr_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.cwun=src.cwun, 
                    target.cwun_ref_compnr=src.cwun_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dino=src.dino, 
                    target.fval=src.fval, 
                    target.huid=src.huid, 
                    target.huid_ref_compnr=src.huid_ref_compnr, 
                    target.imre=src.imre, 
                    target.imrk=src.imrk, 
                    target.irdt=src.irdt, 
                    target.load=src.load, 
                    target.pddt=src.pddt, 
                    target.port=src.port, 
                    target.port_ref_compnr=src.port_ref_compnr, 
                    target.rcno=src.rcno, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.shda=src.shda, 
                    target.shda_ref_compnr=src.shda_ref_compnr, 
                    target.shdt=src.shdt, 
                    target.shid=src.shid, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.tcap=src.tcap, 
                    target.tcap_kw=src.tcap_kw, 
                    target.tccp=src.tccp, 
                    target.tccp_kw=src.tccp_kw, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.trcd=src.trcd, 
                    target.username=src.username, 
                    target.wght=src.wght, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    carr, 
                    carr_ref_compnr, 
                    compnr, 
                    conf, 
                    conf_kw, 
                    crdt, 
                    curr, 
                    curr_ref_compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    cwun, 
                    cwun_ref_compnr, 
                    deleted, 
                    dino, 
                    fval, 
                    huid, 
                    huid_ref_compnr, 
                    imre, 
                    imrk, 
                    irdt, 
                    load, 
                    pddt, 
                    port, 
                    port_ref_compnr, 
                    rcno, 
                    sequencenumber, 
                    sfbp, 
                    sfbp_ref_compnr, 
                    shda, 
                    shda_ref_compnr, 
                    shdt, 
                    shid, 
                    stat, 
                    stat_kw, 
                    tcap, 
                    tcap_kw, 
                    tccp, 
                    tccp_kw, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    trcd, 
                    username, 
                    wght, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.carr, 
                    src.carr_ref_compnr, 
                    src.compnr, 
                    src.conf, 
                    src.conf_kw, 
                    src.crdt, 
                    src.curr, 
                    src.curr_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.cwun, 
                    src.cwun_ref_compnr, 
                    src.deleted, 
                    src.dino, 
                    src.fval, 
                    src.huid, 
                    src.huid_ref_compnr, 
                    src.imre, 
                    src.imrk, 
                    src.irdt, 
                    src.load, 
                    src.pddt, 
                    src.port, 
                    src.port_ref_compnr, 
                    src.rcno, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.shda, 
                    src.shda_ref_compnr, 
                    src.shdt, 
                    src.shid, 
                    src.stat, 
                    src.stat_kw, 
                    src.tcap, 
                    src.tcap_kw, 
                    src.tccp, 
                    src.tccp_kw, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.trcd, 
                    src.username, 
                    src.wght,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINH310_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.carr, 
            strm.carr_ref_compnr, 
            strm.compnr, 
            strm.conf, 
            strm.conf_kw, 
            strm.crdt, 
            strm.curr, 
            strm.curr_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.cwun, 
            strm.cwun_ref_compnr, 
            strm.deleted, 
            strm.dino, 
            strm.fval, 
            strm.huid, 
            strm.huid_ref_compnr, 
            strm.imre, 
            strm.imrk, 
            strm.irdt, 
            strm.load, 
            strm.pddt, 
            strm.port, 
            strm.port_ref_compnr, 
            strm.rcno, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.shda, 
            strm.shda_ref_compnr, 
            strm.shdt, 
            strm.shid, 
            strm.stat, 
            strm.stat_kw, 
            strm.tcap, 
            strm.tcap_kw, 
            strm.tccp, 
            strm.tccp_kw, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.trcd, 
            strm.username, 
            strm.wght, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.rcno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH310 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) 
                when matched then update set
                    target.carr=src.carr, 
                    target.carr_ref_compnr=src.carr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.conf=src.conf, 
                    target.conf_kw=src.conf_kw, 
                    target.crdt=src.crdt, 
                    target.curr=src.curr, 
                    target.curr_ref_compnr=src.curr_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.cwun=src.cwun, 
                    target.cwun_ref_compnr=src.cwun_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dino=src.dino, 
                    target.fval=src.fval, 
                    target.huid=src.huid, 
                    target.huid_ref_compnr=src.huid_ref_compnr, 
                    target.imre=src.imre, 
                    target.imrk=src.imrk, 
                    target.irdt=src.irdt, 
                    target.load=src.load, 
                    target.pddt=src.pddt, 
                    target.port=src.port, 
                    target.port_ref_compnr=src.port_ref_compnr, 
                    target.rcno=src.rcno, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.shda=src.shda, 
                    target.shda_ref_compnr=src.shda_ref_compnr, 
                    target.shdt=src.shdt, 
                    target.shid=src.shid, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.tcap=src.tcap, 
                    target.tcap_kw=src.tcap_kw, 
                    target.tccp=src.tccp, 
                    target.tccp_kw=src.tccp_kw, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.trcd=src.trcd, 
                    target.username=src.username, 
                    target.wght=src.wght, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( carr, carr_ref_compnr, compnr, conf, conf_kw, crdt, curr, curr_ref_compnr, cwar, cwar_ref_compnr, cwun, cwun_ref_compnr, deleted, dino, fval, huid, huid_ref_compnr, imre, imrk, irdt, load, pddt, port, port_ref_compnr, rcno, sequencenumber, sfbp, sfbp_ref_compnr, shda, shda_ref_compnr, shdt, shid, stat, stat_kw, tcap, tcap_kw, tccp, tccp_kw, text, text_ref_compnr, timestamp, trcd, username, wght, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.carr, 
                    src.carr_ref_compnr, 
                    src.compnr, 
                    src.conf, 
                    src.conf_kw, 
                    src.crdt, 
                    src.curr, 
                    src.curr_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.cwun, 
                    src.cwun_ref_compnr, 
                    src.deleted, 
                    src.dino, 
                    src.fval, 
                    src.huid, 
                    src.huid_ref_compnr, 
                    src.imre, 
                    src.imrk, 
                    src.irdt, 
                    src.load, 
                    src.pddt, 
                    src.port, 
                    src.port_ref_compnr, 
                    src.rcno, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.shda, 
                    src.shda_ref_compnr, 
                    src.shdt, 
                    src.shid, 
                    src.stat, 
                    src.stat_kw, 
                    src.tcap, 
                    src.tcap_kw, 
                    src.tccp, 
                    src.tccp_kw, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.trcd, 
                    src.username, 
                    src.wght,     
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