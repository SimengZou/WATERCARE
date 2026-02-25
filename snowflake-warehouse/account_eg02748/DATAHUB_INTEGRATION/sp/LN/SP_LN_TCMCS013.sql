create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS013()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS013_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS013 as target using (SELECT * FROM (SELECT 
            strm.atie, 
            strm.atie_kw, 
            strm.cdde, 
            strm.cdde_kw, 
            strm.cdis, 
            strm.cdis_kw, 
            strm.compnr, 
            strm.cpay, 
            strm.day1, 
            strm.day2, 
            strm.day3, 
            strm.deleted, 
            strm.disa, 
            strm.disb, 
            strm.disc, 
            strm.dsca, 
            strm.dtbs, 
            strm.dtbs_kw, 
            strm.fdis, 
            strm.fdue, 
            strm.pash, 
            strm.pash_ref_compnr, 
            strm.pdin, 
            strm.pdin_kw, 
            strm.pdis, 
            strm.pdis_kw, 
            strm.pdyn, 
            strm.pdyn_kw, 
            strm.pper, 
            strm.prca, 
            strm.prcb, 
            strm.prcc, 
            strm.prio, 
            strm.prio_kw, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.tlsd, 
            strm.tola, 
            strm.told, 
            strm.tolp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vtpm, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpay ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS013 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpay=src.cpay) OR (target.cpay IS NULL AND src.cpay IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.atie=src.atie, 
                    target.atie_kw=src.atie_kw, 
                    target.cdde=src.cdde, 
                    target.cdde_kw=src.cdde_kw, 
                    target.cdis=src.cdis, 
                    target.cdis_kw=src.cdis_kw, 
                    target.compnr=src.compnr, 
                    target.cpay=src.cpay, 
                    target.day1=src.day1, 
                    target.day2=src.day2, 
                    target.day3=src.day3, 
                    target.deleted=src.deleted, 
                    target.disa=src.disa, 
                    target.disb=src.disb, 
                    target.disc=src.disc, 
                    target.dsca=src.dsca, 
                    target.dtbs=src.dtbs, 
                    target.dtbs_kw=src.dtbs_kw, 
                    target.fdis=src.fdis, 
                    target.fdue=src.fdue, 
                    target.pash=src.pash, 
                    target.pash_ref_compnr=src.pash_ref_compnr, 
                    target.pdin=src.pdin, 
                    target.pdin_kw=src.pdin_kw, 
                    target.pdis=src.pdis, 
                    target.pdis_kw=src.pdis_kw, 
                    target.pdyn=src.pdyn, 
                    target.pdyn_kw=src.pdyn_kw, 
                    target.pper=src.pper, 
                    target.prca=src.prca, 
                    target.prcb=src.prcb, 
                    target.prcc=src.prcc, 
                    target.prio=src.prio, 
                    target.prio_kw=src.prio_kw, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.tlsd=src.tlsd, 
                    target.tola=src.tola, 
                    target.told=src.told, 
                    target.tolp=src.tolp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vtpm=src.vtpm, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    atie, 
                    atie_kw, 
                    cdde, 
                    cdde_kw, 
                    cdis, 
                    cdis_kw, 
                    compnr, 
                    cpay, 
                    day1, 
                    day2, 
                    day3, 
                    deleted, 
                    disa, 
                    disb, 
                    disc, 
                    dsca, 
                    dtbs, 
                    dtbs_kw, 
                    fdis, 
                    fdue, 
                    pash, 
                    pash_ref_compnr, 
                    pdin, 
                    pdin_kw, 
                    pdis, 
                    pdis_kw, 
                    pdyn, 
                    pdyn_kw, 
                    pper, 
                    prca, 
                    prcb, 
                    prcc, 
                    prio, 
                    prio_kw, 
                    ptyp, 
                    ptyp_kw, 
                    sequencenumber, 
                    timestamp, 
                    tlsd, 
                    tola, 
                    told, 
                    tolp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    vtpm, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.atie, 
                    src.atie_kw, 
                    src.cdde, 
                    src.cdde_kw, 
                    src.cdis, 
                    src.cdis_kw, 
                    src.compnr, 
                    src.cpay, 
                    src.day1, 
                    src.day2, 
                    src.day3, 
                    src.deleted, 
                    src.disa, 
                    src.disb, 
                    src.disc, 
                    src.dsca, 
                    src.dtbs, 
                    src.dtbs_kw, 
                    src.fdis, 
                    src.fdue, 
                    src.pash, 
                    src.pash_ref_compnr, 
                    src.pdin, 
                    src.pdin_kw, 
                    src.pdis, 
                    src.pdis_kw, 
                    src.pdyn, 
                    src.pdyn_kw, 
                    src.pper, 
                    src.prca, 
                    src.prcb, 
                    src.prcc, 
                    src.prio, 
                    src.prio_kw, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.tlsd, 
                    src.tola, 
                    src.told, 
                    src.tolp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.vtpm,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS013_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.atie, 
            strm.atie_kw, 
            strm.cdde, 
            strm.cdde_kw, 
            strm.cdis, 
            strm.cdis_kw, 
            strm.compnr, 
            strm.cpay, 
            strm.day1, 
            strm.day2, 
            strm.day3, 
            strm.deleted, 
            strm.disa, 
            strm.disb, 
            strm.disc, 
            strm.dsca, 
            strm.dtbs, 
            strm.dtbs_kw, 
            strm.fdis, 
            strm.fdue, 
            strm.pash, 
            strm.pash_ref_compnr, 
            strm.pdin, 
            strm.pdin_kw, 
            strm.pdis, 
            strm.pdis_kw, 
            strm.pdyn, 
            strm.pdyn_kw, 
            strm.pper, 
            strm.prca, 
            strm.prcb, 
            strm.prcc, 
            strm.prio, 
            strm.prio_kw, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.tlsd, 
            strm.tola, 
            strm.told, 
            strm.tolp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vtpm, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpay ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS013 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpay=src.cpay) OR (target.cpay IS NULL AND src.cpay IS NULL)) 
                when matched then update set
                    target.atie=src.atie, 
                    target.atie_kw=src.atie_kw, 
                    target.cdde=src.cdde, 
                    target.cdde_kw=src.cdde_kw, 
                    target.cdis=src.cdis, 
                    target.cdis_kw=src.cdis_kw, 
                    target.compnr=src.compnr, 
                    target.cpay=src.cpay, 
                    target.day1=src.day1, 
                    target.day2=src.day2, 
                    target.day3=src.day3, 
                    target.deleted=src.deleted, 
                    target.disa=src.disa, 
                    target.disb=src.disb, 
                    target.disc=src.disc, 
                    target.dsca=src.dsca, 
                    target.dtbs=src.dtbs, 
                    target.dtbs_kw=src.dtbs_kw, 
                    target.fdis=src.fdis, 
                    target.fdue=src.fdue, 
                    target.pash=src.pash, 
                    target.pash_ref_compnr=src.pash_ref_compnr, 
                    target.pdin=src.pdin, 
                    target.pdin_kw=src.pdin_kw, 
                    target.pdis=src.pdis, 
                    target.pdis_kw=src.pdis_kw, 
                    target.pdyn=src.pdyn, 
                    target.pdyn_kw=src.pdyn_kw, 
                    target.pper=src.pper, 
                    target.prca=src.prca, 
                    target.prcb=src.prcb, 
                    target.prcc=src.prcc, 
                    target.prio=src.prio, 
                    target.prio_kw=src.prio_kw, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.tlsd=src.tlsd, 
                    target.tola=src.tola, 
                    target.told=src.told, 
                    target.tolp=src.tolp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vtpm=src.vtpm, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( atie, atie_kw, cdde, cdde_kw, cdis, cdis_kw, compnr, cpay, day1, day2, day3, deleted, disa, disb, disc, dsca, dtbs, dtbs_kw, fdis, fdue, pash, pash_ref_compnr, pdin, pdin_kw, pdis, pdis_kw, pdyn, pdyn_kw, pper, prca, prcb, prcc, prio, prio_kw, ptyp, ptyp_kw, sequencenumber, timestamp, tlsd, tola, told, tolp, txta, txta_ref_compnr, username, vtpm, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.atie, 
                    src.atie_kw, 
                    src.cdde, 
                    src.cdde_kw, 
                    src.cdis, 
                    src.cdis_kw, 
                    src.compnr, 
                    src.cpay, 
                    src.day1, 
                    src.day2, 
                    src.day3, 
                    src.deleted, 
                    src.disa, 
                    src.disb, 
                    src.disc, 
                    src.dsca, 
                    src.dtbs, 
                    src.dtbs_kw, 
                    src.fdis, 
                    src.fdue, 
                    src.pash, 
                    src.pash_ref_compnr, 
                    src.pdin, 
                    src.pdin_kw, 
                    src.pdis, 
                    src.pdis_kw, 
                    src.pdyn, 
                    src.pdyn_kw, 
                    src.pper, 
                    src.prca, 
                    src.prcb, 
                    src.prcc, 
                    src.prio, 
                    src.prio_kw, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.tlsd, 
                    src.tola, 
                    src.told, 
                    src.tolp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.vtpm,     
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