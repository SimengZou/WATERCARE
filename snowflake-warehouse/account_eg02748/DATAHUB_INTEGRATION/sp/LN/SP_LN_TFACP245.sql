create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFACP245()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFACP245_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFACP245 as target using (SELECT * FROM (SELECT 
            strm.boid, 
            strm.bonm, 
            strm.borf, 
            strm.cmnf, 
            strm.compnr, 
            strm.cval, 
            strm.cvlc, 
            strm.cvlc_ref_compnr, 
            strm.deleted, 
            strm.fcpo, 
            strm.imrf, 
            strm.lmat, 
            strm.lmat_kw, 
            strm.loco, 
            strm.loco_otyp_orno_pono_sqnb_ref_compnr, 
            strm.maam, 
            strm.maqu, 
            strm.mpnr, 
            strm.mqpu, 
            strm.mtch, 
            strm.mtch_kw, 
            strm.omti, 
            strm.omti_kw, 
            strm.orno, 
            strm.otbp, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pono, 
            strm.quan, 
            strm.rcno, 
            strm.rdat, 
            strm.rgui, 
            strm.rqty, 
            strm.rseq, 
            strm.sbdt, 
            strm.sbdt_kw, 
            strm.sdat, 
            strm.sequencenumber, 
            strm.sfbl, 
            strm.sfbl_kw, 
            strm.shpm, 
            strm.sqnb, 
            strm.timestamp, 
            strm.toma, 
            strm.toma_kw, 
            strm.trqn, 
            strm.uppr, 
            strm.uppr_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.shpm,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb,
            strm.loco,
            strm.rcno,
            strm.rseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP245 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) AND
            ((target.rseq=src.rseq) OR (target.rseq IS NULL AND src.rseq IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.boid=src.boid, 
                    target.bonm=src.bonm, 
                    target.borf=src.borf, 
                    target.cmnf=src.cmnf, 
                    target.compnr=src.compnr, 
                    target.cval=src.cval, 
                    target.cvlc=src.cvlc, 
                    target.cvlc_ref_compnr=src.cvlc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.fcpo=src.fcpo, 
                    target.imrf=src.imrf, 
                    target.lmat=src.lmat, 
                    target.lmat_kw=src.lmat_kw, 
                    target.loco=src.loco, 
                    target.loco_otyp_orno_pono_sqnb_ref_compnr=src.loco_otyp_orno_pono_sqnb_ref_compnr, 
                    target.maam=src.maam, 
                    target.maqu=src.maqu, 
                    target.mpnr=src.mpnr, 
                    target.mqpu=src.mqpu, 
                    target.mtch=src.mtch, 
                    target.mtch_kw=src.mtch_kw, 
                    target.omti=src.omti, 
                    target.omti_kw=src.omti_kw, 
                    target.orno=src.orno, 
                    target.otbp=src.otbp, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pono=src.pono, 
                    target.quan=src.quan, 
                    target.rcno=src.rcno, 
                    target.rdat=src.rdat, 
                    target.rgui=src.rgui, 
                    target.rqty=src.rqty, 
                    target.rseq=src.rseq, 
                    target.sbdt=src.sbdt, 
                    target.sbdt_kw=src.sbdt_kw, 
                    target.sdat=src.sdat, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbl=src.sfbl, 
                    target.sfbl_kw=src.sfbl_kw, 
                    target.shpm=src.shpm, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.toma=src.toma, 
                    target.toma_kw=src.toma_kw, 
                    target.trqn=src.trqn, 
                    target.uppr=src.uppr, 
                    target.uppr_kw=src.uppr_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    boid, 
                    bonm, 
                    borf, 
                    cmnf, 
                    compnr, 
                    cval, 
                    cvlc, 
                    cvlc_ref_compnr, 
                    deleted, 
                    fcpo, 
                    imrf, 
                    lmat, 
                    lmat_kw, 
                    loco, 
                    loco_otyp_orno_pono_sqnb_ref_compnr, 
                    maam, 
                    maqu, 
                    mpnr, 
                    mqpu, 
                    mtch, 
                    mtch_kw, 
                    omti, 
                    omti_kw, 
                    orno, 
                    otbp, 
                    otyp, 
                    otyp_kw, 
                    pono, 
                    quan, 
                    rcno, 
                    rdat, 
                    rgui, 
                    rqty, 
                    rseq, 
                    sbdt, 
                    sbdt_kw, 
                    sdat, 
                    sequencenumber, 
                    sfbl, 
                    sfbl_kw, 
                    shpm, 
                    sqnb, 
                    timestamp, 
                    toma, 
                    toma_kw, 
                    trqn, 
                    uppr, 
                    uppr_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.boid, 
                    src.bonm, 
                    src.borf, 
                    src.cmnf, 
                    src.compnr, 
                    src.cval, 
                    src.cvlc, 
                    src.cvlc_ref_compnr, 
                    src.deleted, 
                    src.fcpo, 
                    src.imrf, 
                    src.lmat, 
                    src.lmat_kw, 
                    src.loco, 
                    src.loco_otyp_orno_pono_sqnb_ref_compnr, 
                    src.maam, 
                    src.maqu, 
                    src.mpnr, 
                    src.mqpu, 
                    src.mtch, 
                    src.mtch_kw, 
                    src.omti, 
                    src.omti_kw, 
                    src.orno, 
                    src.otbp, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pono, 
                    src.quan, 
                    src.rcno, 
                    src.rdat, 
                    src.rgui, 
                    src.rqty, 
                    src.rseq, 
                    src.sbdt, 
                    src.sbdt_kw, 
                    src.sdat, 
                    src.sequencenumber, 
                    src.sfbl, 
                    src.sfbl_kw, 
                    src.shpm, 
                    src.sqnb, 
                    src.timestamp, 
                    src.toma, 
                    src.toma_kw, 
                    src.trqn, 
                    src.uppr, 
                    src.uppr_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFACP245_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.boid, 
            strm.bonm, 
            strm.borf, 
            strm.cmnf, 
            strm.compnr, 
            strm.cval, 
            strm.cvlc, 
            strm.cvlc_ref_compnr, 
            strm.deleted, 
            strm.fcpo, 
            strm.imrf, 
            strm.lmat, 
            strm.lmat_kw, 
            strm.loco, 
            strm.loco_otyp_orno_pono_sqnb_ref_compnr, 
            strm.maam, 
            strm.maqu, 
            strm.mpnr, 
            strm.mqpu, 
            strm.mtch, 
            strm.mtch_kw, 
            strm.omti, 
            strm.omti_kw, 
            strm.orno, 
            strm.otbp, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pono, 
            strm.quan, 
            strm.rcno, 
            strm.rdat, 
            strm.rgui, 
            strm.rqty, 
            strm.rseq, 
            strm.sbdt, 
            strm.sbdt_kw, 
            strm.sdat, 
            strm.sequencenumber, 
            strm.sfbl, 
            strm.sfbl_kw, 
            strm.shpm, 
            strm.sqnb, 
            strm.timestamp, 
            strm.toma, 
            strm.toma_kw, 
            strm.trqn, 
            strm.uppr, 
            strm.uppr_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.shpm,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb,
            strm.loco,
            strm.rcno,
            strm.rseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP245 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) AND
            ((target.rseq=src.rseq) OR (target.rseq IS NULL AND src.rseq IS NULL)) 
                when matched then update set
                    target.boid=src.boid, 
                    target.bonm=src.bonm, 
                    target.borf=src.borf, 
                    target.cmnf=src.cmnf, 
                    target.compnr=src.compnr, 
                    target.cval=src.cval, 
                    target.cvlc=src.cvlc, 
                    target.cvlc_ref_compnr=src.cvlc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.fcpo=src.fcpo, 
                    target.imrf=src.imrf, 
                    target.lmat=src.lmat, 
                    target.lmat_kw=src.lmat_kw, 
                    target.loco=src.loco, 
                    target.loco_otyp_orno_pono_sqnb_ref_compnr=src.loco_otyp_orno_pono_sqnb_ref_compnr, 
                    target.maam=src.maam, 
                    target.maqu=src.maqu, 
                    target.mpnr=src.mpnr, 
                    target.mqpu=src.mqpu, 
                    target.mtch=src.mtch, 
                    target.mtch_kw=src.mtch_kw, 
                    target.omti=src.omti, 
                    target.omti_kw=src.omti_kw, 
                    target.orno=src.orno, 
                    target.otbp=src.otbp, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pono=src.pono, 
                    target.quan=src.quan, 
                    target.rcno=src.rcno, 
                    target.rdat=src.rdat, 
                    target.rgui=src.rgui, 
                    target.rqty=src.rqty, 
                    target.rseq=src.rseq, 
                    target.sbdt=src.sbdt, 
                    target.sbdt_kw=src.sbdt_kw, 
                    target.sdat=src.sdat, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbl=src.sfbl, 
                    target.sfbl_kw=src.sfbl_kw, 
                    target.shpm=src.shpm, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.toma=src.toma, 
                    target.toma_kw=src.toma_kw, 
                    target.trqn=src.trqn, 
                    target.uppr=src.uppr, 
                    target.uppr_kw=src.uppr_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( boid, bonm, borf, cmnf, compnr, cval, cvlc, cvlc_ref_compnr, deleted, fcpo, imrf, lmat, lmat_kw, loco, loco_otyp_orno_pono_sqnb_ref_compnr, maam, maqu, mpnr, mqpu, mtch, mtch_kw, omti, omti_kw, orno, otbp, otyp, otyp_kw, pono, quan, rcno, rdat, rgui, rqty, rseq, sbdt, sbdt_kw, sdat, sequencenumber, sfbl, sfbl_kw, shpm, sqnb, timestamp, toma, toma_kw, trqn, uppr, uppr_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.boid, 
                    src.bonm, 
                    src.borf, 
                    src.cmnf, 
                    src.compnr, 
                    src.cval, 
                    src.cvlc, 
                    src.cvlc_ref_compnr, 
                    src.deleted, 
                    src.fcpo, 
                    src.imrf, 
                    src.lmat, 
                    src.lmat_kw, 
                    src.loco, 
                    src.loco_otyp_orno_pono_sqnb_ref_compnr, 
                    src.maam, 
                    src.maqu, 
                    src.mpnr, 
                    src.mqpu, 
                    src.mtch, 
                    src.mtch_kw, 
                    src.omti, 
                    src.omti_kw, 
                    src.orno, 
                    src.otbp, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pono, 
                    src.quan, 
                    src.rcno, 
                    src.rdat, 
                    src.rgui, 
                    src.rqty, 
                    src.rseq, 
                    src.sbdt, 
                    src.sbdt_kw, 
                    src.sdat, 
                    src.sequencenumber, 
                    src.sfbl, 
                    src.sfbl_kw, 
                    src.shpm, 
                    src.sqnb, 
                    src.timestamp, 
                    src.toma, 
                    src.toma_kw, 
                    src.trqn, 
                    src.uppr, 
                    src.uppr_kw, 
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