create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD010()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD010_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD010 as target using (SELECT * FROM (SELECT 
            strm.atyp, 
            strm.atyp_kw, 
            strm.bfdt, 
            strm.bloc, 
            strm.bloc_kw, 
            strm.budt, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.dimx, 
            strm.dtyp, 
            strm.dtyp_pdix_ref_compnr, 
            strm.dtyp_ref_compnr, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.pdix, 
            strm.pseq, 
            strm.qan1, 
            strm.qan1_ref_compnr, 
            strm.qan2, 
            strm.qan2_ref_compnr, 
            strm.sequencenumber, 
            strm.skey, 
            strm.subl, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.uni1, 
            strm.uni1_kw, 
            strm.uni2, 
            strm.uni2_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.dtyp,
            strm.dimx ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD010 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.dtyp=src.dtyp) OR (target.dtyp IS NULL AND src.dtyp IS NULL)) AND
            ((target.dimx=src.dimx) OR (target.dimx IS NULL AND src.dimx IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.atyp=src.atyp, 
                    target.atyp_kw=src.atyp_kw, 
                    target.bfdt=src.bfdt, 
                    target.bloc=src.bloc, 
                    target.bloc_kw=src.bloc_kw, 
                    target.budt=src.budt, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.dimx=src.dimx, 
                    target.dtyp=src.dtyp, 
                    target.dtyp_pdix_ref_compnr=src.dtyp_pdix_ref_compnr, 
                    target.dtyp_ref_compnr=src.dtyp_ref_compnr, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.pdix=src.pdix, 
                    target.pseq=src.pseq, 
                    target.qan1=src.qan1, 
                    target.qan1_ref_compnr=src.qan1_ref_compnr, 
                    target.qan2=src.qan2, 
                    target.qan2_ref_compnr=src.qan2_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.skey=src.skey, 
                    target.subl=src.subl, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.uni1=src.uni1, 
                    target.uni1_kw=src.uni1_kw, 
                    target.uni2=src.uni2, 
                    target.uni2_kw=src.uni2_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    atyp, 
                    atyp_kw, 
                    bfdt, 
                    bloc, 
                    bloc_kw, 
                    budt, 
                    compnr, 
                    deleted, 
                    desc, 
                    dimx, 
                    dtyp, 
                    dtyp_pdix_ref_compnr, 
                    dtyp_ref_compnr, 
                    emno, 
                    emno_ref_compnr, 
                    pdix, 
                    pseq, 
                    qan1, 
                    qan1_ref_compnr, 
                    qan2, 
                    qan2_ref_compnr, 
                    sequencenumber, 
                    skey, 
                    subl, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    uni1, 
                    uni1_kw, 
                    uni2, 
                    uni2_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.atyp, 
                    src.atyp_kw, 
                    src.bfdt, 
                    src.bloc, 
                    src.bloc_kw, 
                    src.budt, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.dimx, 
                    src.dtyp, 
                    src.dtyp_pdix_ref_compnr, 
                    src.dtyp_ref_compnr, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.pdix, 
                    src.pseq, 
                    src.qan1, 
                    src.qan1_ref_compnr, 
                    src.qan2, 
                    src.qan2_ref_compnr, 
                    src.sequencenumber, 
                    src.skey, 
                    src.subl, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.uni1, 
                    src.uni1_kw, 
                    src.uni2, 
                    src.uni2_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD010_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.atyp, 
            strm.atyp_kw, 
            strm.bfdt, 
            strm.bloc, 
            strm.bloc_kw, 
            strm.budt, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.dimx, 
            strm.dtyp, 
            strm.dtyp_pdix_ref_compnr, 
            strm.dtyp_ref_compnr, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.pdix, 
            strm.pseq, 
            strm.qan1, 
            strm.qan1_ref_compnr, 
            strm.qan2, 
            strm.qan2_ref_compnr, 
            strm.sequencenumber, 
            strm.skey, 
            strm.subl, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.uni1, 
            strm.uni1_kw, 
            strm.uni2, 
            strm.uni2_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.dtyp,
            strm.dimx ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD010 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.dtyp=src.dtyp) OR (target.dtyp IS NULL AND src.dtyp IS NULL)) AND
            ((target.dimx=src.dimx) OR (target.dimx IS NULL AND src.dimx IS NULL)) 
                when matched then update set
                    target.atyp=src.atyp, 
                    target.atyp_kw=src.atyp_kw, 
                    target.bfdt=src.bfdt, 
                    target.bloc=src.bloc, 
                    target.bloc_kw=src.bloc_kw, 
                    target.budt=src.budt, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.dimx=src.dimx, 
                    target.dtyp=src.dtyp, 
                    target.dtyp_pdix_ref_compnr=src.dtyp_pdix_ref_compnr, 
                    target.dtyp_ref_compnr=src.dtyp_ref_compnr, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.pdix=src.pdix, 
                    target.pseq=src.pseq, 
                    target.qan1=src.qan1, 
                    target.qan1_ref_compnr=src.qan1_ref_compnr, 
                    target.qan2=src.qan2, 
                    target.qan2_ref_compnr=src.qan2_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.skey=src.skey, 
                    target.subl=src.subl, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.uni1=src.uni1, 
                    target.uni1_kw=src.uni1_kw, 
                    target.uni2=src.uni2, 
                    target.uni2_kw=src.uni2_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( atyp, atyp_kw, bfdt, bloc, bloc_kw, budt, compnr, deleted, desc, dimx, dtyp, dtyp_pdix_ref_compnr, dtyp_ref_compnr, emno, emno_ref_compnr, pdix, pseq, qan1, qan1_ref_compnr, qan2, qan2_ref_compnr, sequencenumber, skey, subl, text, text_ref_compnr, timestamp, uni1, uni1_kw, uni2, uni2_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.atyp, 
                    src.atyp_kw, 
                    src.bfdt, 
                    src.bloc, 
                    src.bloc_kw, 
                    src.budt, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.dimx, 
                    src.dtyp, 
                    src.dtyp_pdix_ref_compnr, 
                    src.dtyp_ref_compnr, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.pdix, 
                    src.pseq, 
                    src.qan1, 
                    src.qan1_ref_compnr, 
                    src.qan2, 
                    src.qan2_ref_compnr, 
                    src.sequencenumber, 
                    src.skey, 
                    src.subl, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.uni1, 
                    src.uni1_kw, 
                    src.uni2, 
                    src.uni2_kw, 
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