create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFACP600()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFACP600_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFACP600 as target using (SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amtl, 
            strm.bank, 
            strm.basu, 
            strm.compnr, 
            strm.curr, 
            strm.ddat, 
            strm.deleted, 
            strm.gcom, 
            strm.gdoc, 
            strm.glin, 
            strm.gtyp, 
            strm.ipco, 
            strm.ipdo, 
            strm.ipli, 
            strm.ipty, 
            strm.payd, 
            strm.payl, 
            strm.paym, 
            strm.payt, 
            strm.pbcp, 
            strm.pbtn, 
            strm.pcom, 
            strm.ptbp, 
            strm.ptbp_ref_compnr, 
            strm.pyid, 
            strm.reas, 
            strm.sdat, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.step, 
            strm.step_kw, 
            strm.timestamp, 
            strm.tpay, 
            strm.tpay_kw, 
            strm.tpbp, 
            strm.tpbp_ref_compnr, 
            strm.username, 
            strm.usrc, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pcom,
            strm.payt,
            strm.payd,
            strm.payl,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP600 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pcom=src.pcom) OR (target.pcom IS NULL AND src.pcom IS NULL)) AND
            ((target.payt=src.payt) OR (target.payt IS NULL AND src.payt IS NULL)) AND
            ((target.payd=src.payd) OR (target.payd IS NULL AND src.payd IS NULL)) AND
            ((target.payl=src.payl) OR (target.payl IS NULL AND src.payl IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amtl=src.amtl, 
                    target.bank=src.bank, 
                    target.basu=src.basu, 
                    target.compnr=src.compnr, 
                    target.curr=src.curr, 
                    target.ddat=src.ddat, 
                    target.deleted=src.deleted, 
                    target.gcom=src.gcom, 
                    target.gdoc=src.gdoc, 
                    target.glin=src.glin, 
                    target.gtyp=src.gtyp, 
                    target.ipco=src.ipco, 
                    target.ipdo=src.ipdo, 
                    target.ipli=src.ipli, 
                    target.ipty=src.ipty, 
                    target.payd=src.payd, 
                    target.payl=src.payl, 
                    target.paym=src.paym, 
                    target.payt=src.payt, 
                    target.pbcp=src.pbcp, 
                    target.pbtn=src.pbtn, 
                    target.pcom=src.pcom, 
                    target.ptbp=src.ptbp, 
                    target.ptbp_ref_compnr=src.ptbp_ref_compnr, 
                    target.pyid=src.pyid, 
                    target.reas=src.reas, 
                    target.sdat=src.sdat, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.step=src.step, 
                    target.step_kw=src.step_kw, 
                    target.timestamp=src.timestamp, 
                    target.tpay=src.tpay, 
                    target.tpay_kw=src.tpay_kw, 
                    target.tpbp=src.tpbp, 
                    target.tpbp_ref_compnr=src.tpbp_ref_compnr, 
                    target.username=src.username, 
                    target.usrc=src.usrc, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amnt, 
                    amtl, 
                    bank, 
                    basu, 
                    compnr, 
                    curr, 
                    ddat, 
                    deleted, 
                    gcom, 
                    gdoc, 
                    glin, 
                    gtyp, 
                    ipco, 
                    ipdo, 
                    ipli, 
                    ipty, 
                    payd, 
                    payl, 
                    paym, 
                    payt, 
                    pbcp, 
                    pbtn, 
                    pcom, 
                    ptbp, 
                    ptbp_ref_compnr, 
                    pyid, 
                    reas, 
                    sdat, 
                    seqn, 
                    sequencenumber, 
                    step, 
                    step_kw, 
                    timestamp, 
                    tpay, 
                    tpay_kw, 
                    tpbp, 
                    tpbp_ref_compnr, 
                    username, 
                    usrc, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amnt, 
                    src.amtl, 
                    src.bank, 
                    src.basu, 
                    src.compnr, 
                    src.curr, 
                    src.ddat, 
                    src.deleted, 
                    src.gcom, 
                    src.gdoc, 
                    src.glin, 
                    src.gtyp, 
                    src.ipco, 
                    src.ipdo, 
                    src.ipli, 
                    src.ipty, 
                    src.payd, 
                    src.payl, 
                    src.paym, 
                    src.payt, 
                    src.pbcp, 
                    src.pbtn, 
                    src.pcom, 
                    src.ptbp, 
                    src.ptbp_ref_compnr, 
                    src.pyid, 
                    src.reas, 
                    src.sdat, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.step, 
                    src.step_kw, 
                    src.timestamp, 
                    src.tpay, 
                    src.tpay_kw, 
                    src.tpbp, 
                    src.tpbp_ref_compnr, 
                    src.username, 
                    src.usrc,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFACP600_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amtl, 
            strm.bank, 
            strm.basu, 
            strm.compnr, 
            strm.curr, 
            strm.ddat, 
            strm.deleted, 
            strm.gcom, 
            strm.gdoc, 
            strm.glin, 
            strm.gtyp, 
            strm.ipco, 
            strm.ipdo, 
            strm.ipli, 
            strm.ipty, 
            strm.payd, 
            strm.payl, 
            strm.paym, 
            strm.payt, 
            strm.pbcp, 
            strm.pbtn, 
            strm.pcom, 
            strm.ptbp, 
            strm.ptbp_ref_compnr, 
            strm.pyid, 
            strm.reas, 
            strm.sdat, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.step, 
            strm.step_kw, 
            strm.timestamp, 
            strm.tpay, 
            strm.tpay_kw, 
            strm.tpbp, 
            strm.tpbp_ref_compnr, 
            strm.username, 
            strm.usrc, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pcom,
            strm.payt,
            strm.payd,
            strm.payl,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP600 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pcom=src.pcom) OR (target.pcom IS NULL AND src.pcom IS NULL)) AND
            ((target.payt=src.payt) OR (target.payt IS NULL AND src.payt IS NULL)) AND
            ((target.payd=src.payd) OR (target.payd IS NULL AND src.payd IS NULL)) AND
            ((target.payl=src.payl) OR (target.payl IS NULL AND src.payl IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amtl=src.amtl, 
                    target.bank=src.bank, 
                    target.basu=src.basu, 
                    target.compnr=src.compnr, 
                    target.curr=src.curr, 
                    target.ddat=src.ddat, 
                    target.deleted=src.deleted, 
                    target.gcom=src.gcom, 
                    target.gdoc=src.gdoc, 
                    target.glin=src.glin, 
                    target.gtyp=src.gtyp, 
                    target.ipco=src.ipco, 
                    target.ipdo=src.ipdo, 
                    target.ipli=src.ipli, 
                    target.ipty=src.ipty, 
                    target.payd=src.payd, 
                    target.payl=src.payl, 
                    target.paym=src.paym, 
                    target.payt=src.payt, 
                    target.pbcp=src.pbcp, 
                    target.pbtn=src.pbtn, 
                    target.pcom=src.pcom, 
                    target.ptbp=src.ptbp, 
                    target.ptbp_ref_compnr=src.ptbp_ref_compnr, 
                    target.pyid=src.pyid, 
                    target.reas=src.reas, 
                    target.sdat=src.sdat, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.step=src.step, 
                    target.step_kw=src.step_kw, 
                    target.timestamp=src.timestamp, 
                    target.tpay=src.tpay, 
                    target.tpay_kw=src.tpay_kw, 
                    target.tpbp=src.tpbp, 
                    target.tpbp_ref_compnr=src.tpbp_ref_compnr, 
                    target.username=src.username, 
                    target.usrc=src.usrc, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amnt, amtl, bank, basu, compnr, curr, ddat, deleted, gcom, gdoc, glin, gtyp, ipco, ipdo, ipli, ipty, payd, payl, paym, payt, pbcp, pbtn, pcom, ptbp, ptbp_ref_compnr, pyid, reas, sdat, seqn, sequencenumber, step, step_kw, timestamp, tpay, tpay_kw, tpbp, tpbp_ref_compnr, username, usrc, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amnt, 
                    src.amtl, 
                    src.bank, 
                    src.basu, 
                    src.compnr, 
                    src.curr, 
                    src.ddat, 
                    src.deleted, 
                    src.gcom, 
                    src.gdoc, 
                    src.glin, 
                    src.gtyp, 
                    src.ipco, 
                    src.ipdo, 
                    src.ipli, 
                    src.ipty, 
                    src.payd, 
                    src.payl, 
                    src.paym, 
                    src.payt, 
                    src.pbcp, 
                    src.pbtn, 
                    src.pcom, 
                    src.ptbp, 
                    src.ptbp_ref_compnr, 
                    src.pyid, 
                    src.reas, 
                    src.sdat, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.step, 
                    src.step_kw, 
                    src.timestamp, 
                    src.tpay, 
                    src.tpay_kw, 
                    src.tpbp, 
                    src.tpbp_ref_compnr, 
                    src.username, 
                    src.usrc,     
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