create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS010()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS010_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS010 as target using (SELECT * FROM (SELECT 
            strm.afal, 
            strm.afal_ref_compnr, 
            strm.awtn, 
            strm.awtn_kw, 
            strm.bnch, 
            strm.bnch_kw, 
            strm.ccty, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cgp1, 
            strm.cgp1_ref_compnr, 
            strm.cgp2, 
            strm.cgp2_ref_compnr, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.coaf, 
            strm.coaf_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.esgn, 
            strm.fxcd, 
            strm.geoc, 
            strm.ict2, 
            strm.ictc, 
            strm.ivrc, 
            strm.meec, 
            strm.meec_kw, 
            strm.natl, 
            strm.pltx, 
            strm.pltx_kw, 
            strm.ppcd, 
            strm.ptta, 
            strm.ptta_kw, 
            strm.sequencenumber, 
            strm.ssgn, 
            strm.tfcd, 
            strm.timestamp, 
            strm.txcd, 
            strm.txmp, 
            strm.txmp_kw, 
            strm.tzid, 
            strm.tzid_ref_compnr, 
            strm.uarc, 
            strm.uarc_kw, 
            strm.username, 
            strm.vnch, 
            strm.vnch_kw, 
            strm.xsgn, 
            strm.zmsk, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ccty ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS010 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ccty=src.ccty) OR (target.ccty IS NULL AND src.ccty IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.afal=src.afal, 
                    target.afal_ref_compnr=src.afal_ref_compnr, 
                    target.awtn=src.awtn, 
                    target.awtn_kw=src.awtn_kw, 
                    target.bnch=src.bnch, 
                    target.bnch_kw=src.bnch_kw, 
                    target.ccty=src.ccty, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cgp1=src.cgp1, 
                    target.cgp1_ref_compnr=src.cgp1_ref_compnr, 
                    target.cgp2=src.cgp2, 
                    target.cgp2_ref_compnr=src.cgp2_ref_compnr, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.coaf=src.coaf, 
                    target.coaf_ref_compnr=src.coaf_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.esgn=src.esgn, 
                    target.fxcd=src.fxcd, 
                    target.geoc=src.geoc, 
                    target.ict2=src.ict2, 
                    target.ictc=src.ictc, 
                    target.ivrc=src.ivrc, 
                    target.meec=src.meec, 
                    target.meec_kw=src.meec_kw, 
                    target.natl=src.natl, 
                    target.pltx=src.pltx, 
                    target.pltx_kw=src.pltx_kw, 
                    target.ppcd=src.ppcd, 
                    target.ptta=src.ptta, 
                    target.ptta_kw=src.ptta_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.ssgn=src.ssgn, 
                    target.tfcd=src.tfcd, 
                    target.timestamp=src.timestamp, 
                    target.txcd=src.txcd, 
                    target.txmp=src.txmp, 
                    target.txmp_kw=src.txmp_kw, 
                    target.tzid=src.tzid, 
                    target.tzid_ref_compnr=src.tzid_ref_compnr, 
                    target.uarc=src.uarc, 
                    target.uarc_kw=src.uarc_kw, 
                    target.username=src.username, 
                    target.vnch=src.vnch, 
                    target.vnch_kw=src.vnch_kw, 
                    target.xsgn=src.xsgn, 
                    target.zmsk=src.zmsk, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    afal, 
                    afal_ref_compnr, 
                    awtn, 
                    awtn_kw, 
                    bnch, 
                    bnch_kw, 
                    ccty, 
                    ccur, 
                    ccur_ref_compnr, 
                    cgp1, 
                    cgp1_ref_compnr, 
                    cgp2, 
                    cgp2_ref_compnr, 
                    clan, 
                    clan_ref_compnr, 
                    coaf, 
                    coaf_ref_compnr, 
                    compnr, 
                    deleted, 
                    dsca, 
                    esgn, 
                    fxcd, 
                    geoc, 
                    ict2, 
                    ictc, 
                    ivrc, 
                    meec, 
                    meec_kw, 
                    natl, 
                    pltx, 
                    pltx_kw, 
                    ppcd, 
                    ptta, 
                    ptta_kw, 
                    sequencenumber, 
                    ssgn, 
                    tfcd, 
                    timestamp, 
                    txcd, 
                    txmp, 
                    txmp_kw, 
                    tzid, 
                    tzid_ref_compnr, 
                    uarc, 
                    uarc_kw, 
                    username, 
                    vnch, 
                    vnch_kw, 
                    xsgn, 
                    zmsk, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.afal, 
                    src.afal_ref_compnr, 
                    src.awtn, 
                    src.awtn_kw, 
                    src.bnch, 
                    src.bnch_kw, 
                    src.ccty, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cgp1, 
                    src.cgp1_ref_compnr, 
                    src.cgp2, 
                    src.cgp2_ref_compnr, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.coaf, 
                    src.coaf_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.esgn, 
                    src.fxcd, 
                    src.geoc, 
                    src.ict2, 
                    src.ictc, 
                    src.ivrc, 
                    src.meec, 
                    src.meec_kw, 
                    src.natl, 
                    src.pltx, 
                    src.pltx_kw, 
                    src.ppcd, 
                    src.ptta, 
                    src.ptta_kw, 
                    src.sequencenumber, 
                    src.ssgn, 
                    src.tfcd, 
                    src.timestamp, 
                    src.txcd, 
                    src.txmp, 
                    src.txmp_kw, 
                    src.tzid, 
                    src.tzid_ref_compnr, 
                    src.uarc, 
                    src.uarc_kw, 
                    src.username, 
                    src.vnch, 
                    src.vnch_kw, 
                    src.xsgn, 
                    src.zmsk,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS010_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.afal, 
            strm.afal_ref_compnr, 
            strm.awtn, 
            strm.awtn_kw, 
            strm.bnch, 
            strm.bnch_kw, 
            strm.ccty, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cgp1, 
            strm.cgp1_ref_compnr, 
            strm.cgp2, 
            strm.cgp2_ref_compnr, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.coaf, 
            strm.coaf_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.esgn, 
            strm.fxcd, 
            strm.geoc, 
            strm.ict2, 
            strm.ictc, 
            strm.ivrc, 
            strm.meec, 
            strm.meec_kw, 
            strm.natl, 
            strm.pltx, 
            strm.pltx_kw, 
            strm.ppcd, 
            strm.ptta, 
            strm.ptta_kw, 
            strm.sequencenumber, 
            strm.ssgn, 
            strm.tfcd, 
            strm.timestamp, 
            strm.txcd, 
            strm.txmp, 
            strm.txmp_kw, 
            strm.tzid, 
            strm.tzid_ref_compnr, 
            strm.uarc, 
            strm.uarc_kw, 
            strm.username, 
            strm.vnch, 
            strm.vnch_kw, 
            strm.xsgn, 
            strm.zmsk, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ccty ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS010 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ccty=src.ccty) OR (target.ccty IS NULL AND src.ccty IS NULL)) 
                when matched then update set
                    target.afal=src.afal, 
                    target.afal_ref_compnr=src.afal_ref_compnr, 
                    target.awtn=src.awtn, 
                    target.awtn_kw=src.awtn_kw, 
                    target.bnch=src.bnch, 
                    target.bnch_kw=src.bnch_kw, 
                    target.ccty=src.ccty, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cgp1=src.cgp1, 
                    target.cgp1_ref_compnr=src.cgp1_ref_compnr, 
                    target.cgp2=src.cgp2, 
                    target.cgp2_ref_compnr=src.cgp2_ref_compnr, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.coaf=src.coaf, 
                    target.coaf_ref_compnr=src.coaf_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.esgn=src.esgn, 
                    target.fxcd=src.fxcd, 
                    target.geoc=src.geoc, 
                    target.ict2=src.ict2, 
                    target.ictc=src.ictc, 
                    target.ivrc=src.ivrc, 
                    target.meec=src.meec, 
                    target.meec_kw=src.meec_kw, 
                    target.natl=src.natl, 
                    target.pltx=src.pltx, 
                    target.pltx_kw=src.pltx_kw, 
                    target.ppcd=src.ppcd, 
                    target.ptta=src.ptta, 
                    target.ptta_kw=src.ptta_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.ssgn=src.ssgn, 
                    target.tfcd=src.tfcd, 
                    target.timestamp=src.timestamp, 
                    target.txcd=src.txcd, 
                    target.txmp=src.txmp, 
                    target.txmp_kw=src.txmp_kw, 
                    target.tzid=src.tzid, 
                    target.tzid_ref_compnr=src.tzid_ref_compnr, 
                    target.uarc=src.uarc, 
                    target.uarc_kw=src.uarc_kw, 
                    target.username=src.username, 
                    target.vnch=src.vnch, 
                    target.vnch_kw=src.vnch_kw, 
                    target.xsgn=src.xsgn, 
                    target.zmsk=src.zmsk, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( afal, afal_ref_compnr, awtn, awtn_kw, bnch, bnch_kw, ccty, ccur, ccur_ref_compnr, cgp1, cgp1_ref_compnr, cgp2, cgp2_ref_compnr, clan, clan_ref_compnr, coaf, coaf_ref_compnr, compnr, deleted, dsca, esgn, fxcd, geoc, ict2, ictc, ivrc, meec, meec_kw, natl, pltx, pltx_kw, ppcd, ptta, ptta_kw, sequencenumber, ssgn, tfcd, timestamp, txcd, txmp, txmp_kw, tzid, tzid_ref_compnr, uarc, uarc_kw, username, vnch, vnch_kw, xsgn, zmsk, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.afal, 
                    src.afal_ref_compnr, 
                    src.awtn, 
                    src.awtn_kw, 
                    src.bnch, 
                    src.bnch_kw, 
                    src.ccty, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cgp1, 
                    src.cgp1_ref_compnr, 
                    src.cgp2, 
                    src.cgp2_ref_compnr, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.coaf, 
                    src.coaf_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.esgn, 
                    src.fxcd, 
                    src.geoc, 
                    src.ict2, 
                    src.ictc, 
                    src.ivrc, 
                    src.meec, 
                    src.meec_kw, 
                    src.natl, 
                    src.pltx, 
                    src.pltx_kw, 
                    src.ppcd, 
                    src.ptta, 
                    src.ptta_kw, 
                    src.sequencenumber, 
                    src.ssgn, 
                    src.tfcd, 
                    src.timestamp, 
                    src.txcd, 
                    src.txmp, 
                    src.txmp_kw, 
                    src.tzid, 
                    src.tzid_ref_compnr, 
                    src.uarc, 
                    src.uarc_kw, 
                    src.username, 
                    src.vnch, 
                    src.vnch_kw, 
                    src.xsgn, 
                    src.zmsk,     
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