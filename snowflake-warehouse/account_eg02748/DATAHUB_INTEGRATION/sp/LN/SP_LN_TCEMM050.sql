create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCEMM050()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCEMM050_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCEMM050 as target using (SELECT * FROM (SELECT 
            strm.admo, 
            strm.admo_kw, 
            strm.bpid, 
            strm.bpid_ref_compnr, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.clus, 
            strm.clus_ref_compnr, 
            strm.compnr, 
            strm.crby, 
            strm.crdt, 
            strm.cust, 
            strm.cust_kw, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.eunt, 
            strm.eunt_ref_compnr, 
            strm.expl, 
            strm.expl_kw, 
            strm.imag, 
            strm.inf1, 
            strm.inf2, 
            strm.lcmp, 
            strm.lcmp_ref_compnr, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.psit, 
            strm.psit_ref_compnr, 
            strm.ract, 
            strm.ract_ref_compnr, 
            strm.scat, 
            strm.scat_ref_compnr, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.site, 
            strm.stat, 
            strm.stat_kw, 
            strm.stbp, 
            strm.stbp_ref_compnr, 
            strm.subs, 
            strm.subs_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.xsit, 
            strm.xsit_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.site ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCEMM050 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.site=src.site) OR (target.site IS NULL AND src.site IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.admo=src.admo, 
                    target.admo_kw=src.admo_kw, 
                    target.bpid=src.bpid, 
                    target.bpid_ref_compnr=src.bpid_ref_compnr, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.clus=src.clus, 
                    target.clus_ref_compnr=src.clus_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.crby=src.crby, 
                    target.crdt=src.crdt, 
                    target.cust=src.cust, 
                    target.cust_kw=src.cust_kw, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.eunt=src.eunt, 
                    target.eunt_ref_compnr=src.eunt_ref_compnr, 
                    target.expl=src.expl, 
                    target.expl_kw=src.expl_kw, 
                    target.imag=src.imag, 
                    target.inf1=src.inf1, 
                    target.inf2=src.inf2, 
                    target.lcmp=src.lcmp, 
                    target.lcmp_ref_compnr=src.lcmp_ref_compnr, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.psit=src.psit, 
                    target.psit_ref_compnr=src.psit_ref_compnr, 
                    target.ract=src.ract, 
                    target.ract_ref_compnr=src.ract_ref_compnr, 
                    target.scat=src.scat, 
                    target.scat_ref_compnr=src.scat_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.site=src.site, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.stbp=src.stbp, 
                    target.stbp_ref_compnr=src.stbp_ref_compnr, 
                    target.subs=src.subs, 
                    target.subs_kw=src.subs_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.xsit=src.xsit, 
                    target.xsit_kw=src.xsit_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    admo, 
                    admo_kw, 
                    bpid, 
                    bpid_ref_compnr, 
                    cadr, 
                    cadr_ref_compnr, 
                    ccal, 
                    ccal_ref_compnr, 
                    clus, 
                    clus_ref_compnr, 
                    compnr, 
                    crby, 
                    crdt, 
                    cust, 
                    cust_kw, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    desc, 
                    eunt, 
                    eunt_ref_compnr, 
                    expl, 
                    expl_kw, 
                    imag, 
                    inf1, 
                    inf2, 
                    lcmp, 
                    lcmp_ref_compnr, 
                    ofbp, 
                    ofbp_ref_compnr, 
                    psit, 
                    psit_ref_compnr, 
                    ract, 
                    ract_ref_compnr, 
                    scat, 
                    scat_ref_compnr, 
                    sequencenumber, 
                    sfbp, 
                    sfbp_ref_compnr, 
                    site, 
                    stat, 
                    stat_kw, 
                    stbp, 
                    stbp_ref_compnr, 
                    subs, 
                    subs_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    xsit, 
                    xsit_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.admo, 
                    src.admo_kw, 
                    src.bpid, 
                    src.bpid_ref_compnr, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.clus, 
                    src.clus_ref_compnr, 
                    src.compnr, 
                    src.crby, 
                    src.crdt, 
                    src.cust, 
                    src.cust_kw, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.eunt, 
                    src.eunt_ref_compnr, 
                    src.expl, 
                    src.expl_kw, 
                    src.imag, 
                    src.inf1, 
                    src.inf2, 
                    src.lcmp, 
                    src.lcmp_ref_compnr, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.psit, 
                    src.psit_ref_compnr, 
                    src.ract, 
                    src.ract_ref_compnr, 
                    src.scat, 
                    src.scat_ref_compnr, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.site, 
                    src.stat, 
                    src.stat_kw, 
                    src.stbp, 
                    src.stbp_ref_compnr, 
                    src.subs, 
                    src.subs_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.xsit, 
                    src.xsit_kw,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCEMM050_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.admo, 
            strm.admo_kw, 
            strm.bpid, 
            strm.bpid_ref_compnr, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.clus, 
            strm.clus_ref_compnr, 
            strm.compnr, 
            strm.crby, 
            strm.crdt, 
            strm.cust, 
            strm.cust_kw, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.eunt, 
            strm.eunt_ref_compnr, 
            strm.expl, 
            strm.expl_kw, 
            strm.imag, 
            strm.inf1, 
            strm.inf2, 
            strm.lcmp, 
            strm.lcmp_ref_compnr, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.psit, 
            strm.psit_ref_compnr, 
            strm.ract, 
            strm.ract_ref_compnr, 
            strm.scat, 
            strm.scat_ref_compnr, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.site, 
            strm.stat, 
            strm.stat_kw, 
            strm.stbp, 
            strm.stbp_ref_compnr, 
            strm.subs, 
            strm.subs_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.xsit, 
            strm.xsit_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.site ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCEMM050 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.site=src.site) OR (target.site IS NULL AND src.site IS NULL)) 
                when matched then update set
                    target.admo=src.admo, 
                    target.admo_kw=src.admo_kw, 
                    target.bpid=src.bpid, 
                    target.bpid_ref_compnr=src.bpid_ref_compnr, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.clus=src.clus, 
                    target.clus_ref_compnr=src.clus_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.crby=src.crby, 
                    target.crdt=src.crdt, 
                    target.cust=src.cust, 
                    target.cust_kw=src.cust_kw, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.eunt=src.eunt, 
                    target.eunt_ref_compnr=src.eunt_ref_compnr, 
                    target.expl=src.expl, 
                    target.expl_kw=src.expl_kw, 
                    target.imag=src.imag, 
                    target.inf1=src.inf1, 
                    target.inf2=src.inf2, 
                    target.lcmp=src.lcmp, 
                    target.lcmp_ref_compnr=src.lcmp_ref_compnr, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.psit=src.psit, 
                    target.psit_ref_compnr=src.psit_ref_compnr, 
                    target.ract=src.ract, 
                    target.ract_ref_compnr=src.ract_ref_compnr, 
                    target.scat=src.scat, 
                    target.scat_ref_compnr=src.scat_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.site=src.site, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.stbp=src.stbp, 
                    target.stbp_ref_compnr=src.stbp_ref_compnr, 
                    target.subs=src.subs, 
                    target.subs_kw=src.subs_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.xsit=src.xsit, 
                    target.xsit_kw=src.xsit_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( admo, admo_kw, bpid, bpid_ref_compnr, cadr, cadr_ref_compnr, ccal, ccal_ref_compnr, clus, clus_ref_compnr, compnr, crby, crdt, cust, cust_kw, cwar, cwar_ref_compnr, deleted, desc, eunt, eunt_ref_compnr, expl, expl_kw, imag, inf1, inf2, lcmp, lcmp_ref_compnr, ofbp, ofbp_ref_compnr, psit, psit_ref_compnr, ract, ract_ref_compnr, scat, scat_ref_compnr, sequencenumber, sfbp, sfbp_ref_compnr, site, stat, stat_kw, stbp, stbp_ref_compnr, subs, subs_kw, timestamp, txta, txta_ref_compnr, username, xsit, xsit_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.admo, 
                    src.admo_kw, 
                    src.bpid, 
                    src.bpid_ref_compnr, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.clus, 
                    src.clus_ref_compnr, 
                    src.compnr, 
                    src.crby, 
                    src.crdt, 
                    src.cust, 
                    src.cust_kw, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.eunt, 
                    src.eunt_ref_compnr, 
                    src.expl, 
                    src.expl_kw, 
                    src.imag, 
                    src.inf1, 
                    src.inf2, 
                    src.lcmp, 
                    src.lcmp_ref_compnr, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.psit, 
                    src.psit_ref_compnr, 
                    src.ract, 
                    src.ract_ref_compnr, 
                    src.scat, 
                    src.scat_ref_compnr, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.site, 
                    src.stat, 
                    src.stat_kw, 
                    src.stbp, 
                    src.stbp_ref_compnr, 
                    src.subs, 
                    src.subs_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.xsit, 
                    src.xsit_kw,     
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