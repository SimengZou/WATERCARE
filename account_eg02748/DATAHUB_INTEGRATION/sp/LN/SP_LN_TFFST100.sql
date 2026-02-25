create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFST100()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFST100_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFST100 as target using (SELECT * FROM (SELECT 
            strm.accs, 
            strm.accs_kw, 
            strm.altp, 
            strm.altp_kw, 
            strm.altp_salt_ref_compnr, 
            strm.anhe, 
            strm.anhe_ref_compnr, 
            strm.cagr, 
            strm.cagr_ref_compnr, 
            strm.compnr, 
            strm.cugl, 
            strm.cugl_kw, 
            strm.datc, 
            strm.datm, 
            strm.deleted, 
            strm.desc, 
            strm.fstm, 
            strm.mdfs, 
            strm.nstm, 
            strm.nstm_ref_compnr, 
            strm.ouer, 
            strm.ouer_kw, 
            strm.pann, 
            strm.pann_kw, 
            strm.ptra, 
            strm.ptra_kw, 
            strm.rcur, 
            strm.rcur_ref_compnr, 
            strm.rhis, 
            strm.rhis_kw, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.salt, 
            strm.sequencenumber, 
            strm.sltp, 
            strm.sltp_kw, 
            strm.sltp_stlt_ref_compnr, 
            strm.stat, 
            strm.stat_kw, 
            strm.sthe, 
            strm.sthe_ref_compnr, 
            strm.stlt, 
            strm.styp, 
            strm.styp_kw, 
            strm.taxo, 
            strm.timestamp, 
            strm.txdt, 
            strm.username, 
            strm.usrc, 
            strm.usrm, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST100 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.accs=src.accs, 
                    target.accs_kw=src.accs_kw, 
                    target.altp=src.altp, 
                    target.altp_kw=src.altp_kw, 
                    target.altp_salt_ref_compnr=src.altp_salt_ref_compnr, 
                    target.anhe=src.anhe, 
                    target.anhe_ref_compnr=src.anhe_ref_compnr, 
                    target.cagr=src.cagr, 
                    target.cagr_ref_compnr=src.cagr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cugl=src.cugl, 
                    target.cugl_kw=src.cugl_kw, 
                    target.datc=src.datc, 
                    target.datm=src.datm, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.fstm=src.fstm, 
                    target.mdfs=src.mdfs, 
                    target.nstm=src.nstm, 
                    target.nstm_ref_compnr=src.nstm_ref_compnr, 
                    target.ouer=src.ouer, 
                    target.ouer_kw=src.ouer_kw, 
                    target.pann=src.pann, 
                    target.pann_kw=src.pann_kw, 
                    target.ptra=src.ptra, 
                    target.ptra_kw=src.ptra_kw, 
                    target.rcur=src.rcur, 
                    target.rcur_ref_compnr=src.rcur_ref_compnr, 
                    target.rhis=src.rhis, 
                    target.rhis_kw=src.rhis_kw, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.salt=src.salt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sltp=src.sltp, 
                    target.sltp_kw=src.sltp_kw, 
                    target.sltp_stlt_ref_compnr=src.sltp_stlt_ref_compnr, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.sthe=src.sthe, 
                    target.sthe_ref_compnr=src.sthe_ref_compnr, 
                    target.stlt=src.stlt, 
                    target.styp=src.styp, 
                    target.styp_kw=src.styp_kw, 
                    target.taxo=src.taxo, 
                    target.timestamp=src.timestamp, 
                    target.txdt=src.txdt, 
                    target.username=src.username, 
                    target.usrc=src.usrc, 
                    target.usrm=src.usrm, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    accs, 
                    accs_kw, 
                    altp, 
                    altp_kw, 
                    altp_salt_ref_compnr, 
                    anhe, 
                    anhe_ref_compnr, 
                    cagr, 
                    cagr_ref_compnr, 
                    compnr, 
                    cugl, 
                    cugl_kw, 
                    datc, 
                    datm, 
                    deleted, 
                    desc, 
                    fstm, 
                    mdfs, 
                    nstm, 
                    nstm_ref_compnr, 
                    ouer, 
                    ouer_kw, 
                    pann, 
                    pann_kw, 
                    ptra, 
                    ptra_kw, 
                    rcur, 
                    rcur_ref_compnr, 
                    rhis, 
                    rhis_kw, 
                    rtyp, 
                    rtyp_ref_compnr, 
                    salt, 
                    sequencenumber, 
                    sltp, 
                    sltp_kw, 
                    sltp_stlt_ref_compnr, 
                    stat, 
                    stat_kw, 
                    sthe, 
                    sthe_ref_compnr, 
                    stlt, 
                    styp, 
                    styp_kw, 
                    taxo, 
                    timestamp, 
                    txdt, 
                    username, 
                    usrc, 
                    usrm, 
                    vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.accs, 
                    src.accs_kw, 
                    src.altp, 
                    src.altp_kw, 
                    src.altp_salt_ref_compnr, 
                    src.anhe, 
                    src.anhe_ref_compnr, 
                    src.cagr, 
                    src.cagr_ref_compnr, 
                    src.compnr, 
                    src.cugl, 
                    src.cugl_kw, 
                    src.datc, 
                    src.datm, 
                    src.deleted, 
                    src.desc, 
                    src.fstm, 
                    src.mdfs, 
                    src.nstm, 
                    src.nstm_ref_compnr, 
                    src.ouer, 
                    src.ouer_kw, 
                    src.pann, 
                    src.pann_kw, 
                    src.ptra, 
                    src.ptra_kw, 
                    src.rcur, 
                    src.rcur_ref_compnr, 
                    src.rhis, 
                    src.rhis_kw, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.salt, 
                    src.sequencenumber, 
                    src.sltp, 
                    src.sltp_kw, 
                    src.sltp_stlt_ref_compnr, 
                    src.stat, 
                    src.stat_kw, 
                    src.sthe, 
                    src.sthe_ref_compnr, 
                    src.stlt, 
                    src.styp, 
                    src.styp_kw, 
                    src.taxo, 
                    src.timestamp, 
                    src.txdt, 
                    src.username, 
                    src.usrc, 
                    src.usrm, 
                    src.vers,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFST100_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.accs, 
            strm.accs_kw, 
            strm.altp, 
            strm.altp_kw, 
            strm.altp_salt_ref_compnr, 
            strm.anhe, 
            strm.anhe_ref_compnr, 
            strm.cagr, 
            strm.cagr_ref_compnr, 
            strm.compnr, 
            strm.cugl, 
            strm.cugl_kw, 
            strm.datc, 
            strm.datm, 
            strm.deleted, 
            strm.desc, 
            strm.fstm, 
            strm.mdfs, 
            strm.nstm, 
            strm.nstm_ref_compnr, 
            strm.ouer, 
            strm.ouer_kw, 
            strm.pann, 
            strm.pann_kw, 
            strm.ptra, 
            strm.ptra_kw, 
            strm.rcur, 
            strm.rcur_ref_compnr, 
            strm.rhis, 
            strm.rhis_kw, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.salt, 
            strm.sequencenumber, 
            strm.sltp, 
            strm.sltp_kw, 
            strm.sltp_stlt_ref_compnr, 
            strm.stat, 
            strm.stat_kw, 
            strm.sthe, 
            strm.sthe_ref_compnr, 
            strm.stlt, 
            strm.styp, 
            strm.styp_kw, 
            strm.taxo, 
            strm.timestamp, 
            strm.txdt, 
            strm.username, 
            strm.usrc, 
            strm.usrm, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST100 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) 
                when matched then update set
                    target.accs=src.accs, 
                    target.accs_kw=src.accs_kw, 
                    target.altp=src.altp, 
                    target.altp_kw=src.altp_kw, 
                    target.altp_salt_ref_compnr=src.altp_salt_ref_compnr, 
                    target.anhe=src.anhe, 
                    target.anhe_ref_compnr=src.anhe_ref_compnr, 
                    target.cagr=src.cagr, 
                    target.cagr_ref_compnr=src.cagr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cugl=src.cugl, 
                    target.cugl_kw=src.cugl_kw, 
                    target.datc=src.datc, 
                    target.datm=src.datm, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.fstm=src.fstm, 
                    target.mdfs=src.mdfs, 
                    target.nstm=src.nstm, 
                    target.nstm_ref_compnr=src.nstm_ref_compnr, 
                    target.ouer=src.ouer, 
                    target.ouer_kw=src.ouer_kw, 
                    target.pann=src.pann, 
                    target.pann_kw=src.pann_kw, 
                    target.ptra=src.ptra, 
                    target.ptra_kw=src.ptra_kw, 
                    target.rcur=src.rcur, 
                    target.rcur_ref_compnr=src.rcur_ref_compnr, 
                    target.rhis=src.rhis, 
                    target.rhis_kw=src.rhis_kw, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.salt=src.salt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sltp=src.sltp, 
                    target.sltp_kw=src.sltp_kw, 
                    target.sltp_stlt_ref_compnr=src.sltp_stlt_ref_compnr, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.sthe=src.sthe, 
                    target.sthe_ref_compnr=src.sthe_ref_compnr, 
                    target.stlt=src.stlt, 
                    target.styp=src.styp, 
                    target.styp_kw=src.styp_kw, 
                    target.taxo=src.taxo, 
                    target.timestamp=src.timestamp, 
                    target.txdt=src.txdt, 
                    target.username=src.username, 
                    target.usrc=src.usrc, 
                    target.usrm=src.usrm, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( accs, accs_kw, altp, altp_kw, altp_salt_ref_compnr, anhe, anhe_ref_compnr, cagr, cagr_ref_compnr, compnr, cugl, cugl_kw, datc, datm, deleted, desc, fstm, mdfs, nstm, nstm_ref_compnr, ouer, ouer_kw, pann, pann_kw, ptra, ptra_kw, rcur, rcur_ref_compnr, rhis, rhis_kw, rtyp, rtyp_ref_compnr, salt, sequencenumber, sltp, sltp_kw, sltp_stlt_ref_compnr, stat, stat_kw, sthe, sthe_ref_compnr, stlt, styp, styp_kw, taxo, timestamp, txdt, username, usrc, usrm, vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.accs, 
                    src.accs_kw, 
                    src.altp, 
                    src.altp_kw, 
                    src.altp_salt_ref_compnr, 
                    src.anhe, 
                    src.anhe_ref_compnr, 
                    src.cagr, 
                    src.cagr_ref_compnr, 
                    src.compnr, 
                    src.cugl, 
                    src.cugl_kw, 
                    src.datc, 
                    src.datm, 
                    src.deleted, 
                    src.desc, 
                    src.fstm, 
                    src.mdfs, 
                    src.nstm, 
                    src.nstm_ref_compnr, 
                    src.ouer, 
                    src.ouer_kw, 
                    src.pann, 
                    src.pann_kw, 
                    src.ptra, 
                    src.ptra_kw, 
                    src.rcur, 
                    src.rcur_ref_compnr, 
                    src.rhis, 
                    src.rhis_kw, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.salt, 
                    src.sequencenumber, 
                    src.sltp, 
                    src.sltp_kw, 
                    src.sltp_stlt_ref_compnr, 
                    src.stat, 
                    src.stat_kw, 
                    src.sthe, 
                    src.sthe_ref_compnr, 
                    src.stlt, 
                    src.styp, 
                    src.styp_kw, 
                    src.taxo, 
                    src.timestamp, 
                    src.txdt, 
                    src.username, 
                    src.usrc, 
                    src.usrm, 
                    src.vers,     
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