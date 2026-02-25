create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS003()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS003_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS003 as target using (SELECT * FROM (SELECT 
            strm.bpid, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.casi, 
            strm.casi_ref_compnr, 
            strm.cdf_updt, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cwar, 
            strm.cwar_eunt, 
            strm.deleted, 
            strm.dsca, 
            strm.duns, 
            strm.eunt_rcmp, 
            strm.imbp, 
            strm.imbp_ref_compnr, 
            strm.imgt, 
            strm.imgt_kw, 
            strm.inep, 
            strm.inep_kw, 
            strm.mesc, 
            strm.mesc_kw, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.pwip, 
            strm.pwip_kw, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.stbp, 
            strm.stbp_ref_compnr, 
            strm.swhu, 
            strm.swhu_kw, 
            strm.timestamp, 
            strm.typw, 
            strm.typw_kw, 
            strm.username, 
            strm.wmsc, 
            strm.wmsc_kw, 
            strm.xsbp, 
            strm.xsbp_ref_compnr, 
            strm.xsit, 
            strm.xsit_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cwar ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS003 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bpid=src.bpid, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.casi=src.casi, 
                    target.casi_ref_compnr=src.casi_ref_compnr, 
                    target.cdf_updt=src.cdf_updt, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_eunt=src.cwar_eunt, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.duns=src.duns, 
                    target.eunt_rcmp=src.eunt_rcmp, 
                    target.imbp=src.imbp, 
                    target.imbp_ref_compnr=src.imbp_ref_compnr, 
                    target.imgt=src.imgt, 
                    target.imgt_kw=src.imgt_kw, 
                    target.inep=src.inep, 
                    target.inep_kw=src.inep_kw, 
                    target.mesc=src.mesc, 
                    target.mesc_kw=src.mesc_kw, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.pwip=src.pwip, 
                    target.pwip_kw=src.pwip_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.stbp=src.stbp, 
                    target.stbp_ref_compnr=src.stbp_ref_compnr, 
                    target.swhu=src.swhu, 
                    target.swhu_kw=src.swhu_kw, 
                    target.timestamp=src.timestamp, 
                    target.typw=src.typw, 
                    target.typw_kw=src.typw_kw, 
                    target.username=src.username, 
                    target.wmsc=src.wmsc, 
                    target.wmsc_kw=src.wmsc_kw, 
                    target.xsbp=src.xsbp, 
                    target.xsbp_ref_compnr=src.xsbp_ref_compnr, 
                    target.xsit=src.xsit, 
                    target.xsit_kw=src.xsit_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bpid, 
                    cadr, 
                    cadr_ref_compnr, 
                    casi, 
                    casi_ref_compnr, 
                    cdf_updt, 
                    clan, 
                    clan_ref_compnr, 
                    comp, 
                    compnr, 
                    cprj, 
                    cprj_ref_compnr, 
                    cwar, 
                    cwar_eunt, 
                    deleted, 
                    dsca, 
                    duns, 
                    eunt_rcmp, 
                    imbp, 
                    imbp_ref_compnr, 
                    imgt, 
                    imgt_kw, 
                    inep, 
                    inep_kw, 
                    mesc, 
                    mesc_kw, 
                    ofbp, 
                    ofbp_ref_compnr, 
                    otbp, 
                    otbp_ref_compnr, 
                    pwip, 
                    pwip_kw, 
                    sequencenumber, 
                    sfbp, 
                    sfbp_ref_compnr, 
                    stbp, 
                    stbp_ref_compnr, 
                    swhu, 
                    swhu_kw, 
                    timestamp, 
                    typw, 
                    typw_kw, 
                    username, 
                    wmsc, 
                    wmsc_kw, 
                    xsbp, 
                    xsbp_ref_compnr, 
                    xsit, 
                    xsit_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bpid, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.casi, 
                    src.casi_ref_compnr, 
                    src.cdf_updt, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cwar, 
                    src.cwar_eunt, 
                    src.deleted, 
                    src.dsca, 
                    src.duns, 
                    src.eunt_rcmp, 
                    src.imbp, 
                    src.imbp_ref_compnr, 
                    src.imgt, 
                    src.imgt_kw, 
                    src.inep, 
                    src.inep_kw, 
                    src.mesc, 
                    src.mesc_kw, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.pwip, 
                    src.pwip_kw, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.stbp, 
                    src.stbp_ref_compnr, 
                    src.swhu, 
                    src.swhu_kw, 
                    src.timestamp, 
                    src.typw, 
                    src.typw_kw, 
                    src.username, 
                    src.wmsc, 
                    src.wmsc_kw, 
                    src.xsbp, 
                    src.xsbp_ref_compnr, 
                    src.xsit, 
                    src.xsit_kw,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS003_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bpid, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.casi, 
            strm.casi_ref_compnr, 
            strm.cdf_updt, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cwar, 
            strm.cwar_eunt, 
            strm.deleted, 
            strm.dsca, 
            strm.duns, 
            strm.eunt_rcmp, 
            strm.imbp, 
            strm.imbp_ref_compnr, 
            strm.imgt, 
            strm.imgt_kw, 
            strm.inep, 
            strm.inep_kw, 
            strm.mesc, 
            strm.mesc_kw, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.pwip, 
            strm.pwip_kw, 
            strm.sequencenumber, 
            strm.sfbp, 
            strm.sfbp_ref_compnr, 
            strm.stbp, 
            strm.stbp_ref_compnr, 
            strm.swhu, 
            strm.swhu_kw, 
            strm.timestamp, 
            strm.typw, 
            strm.typw_kw, 
            strm.username, 
            strm.wmsc, 
            strm.wmsc_kw, 
            strm.xsbp, 
            strm.xsbp_ref_compnr, 
            strm.xsit, 
            strm.xsit_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cwar ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS003 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) 
                when matched then update set
                    target.bpid=src.bpid, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.casi=src.casi, 
                    target.casi_ref_compnr=src.casi_ref_compnr, 
                    target.cdf_updt=src.cdf_updt, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_eunt=src.cwar_eunt, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.duns=src.duns, 
                    target.eunt_rcmp=src.eunt_rcmp, 
                    target.imbp=src.imbp, 
                    target.imbp_ref_compnr=src.imbp_ref_compnr, 
                    target.imgt=src.imgt, 
                    target.imgt_kw=src.imgt_kw, 
                    target.inep=src.inep, 
                    target.inep_kw=src.inep_kw, 
                    target.mesc=src.mesc, 
                    target.mesc_kw=src.mesc_kw, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.pwip=src.pwip, 
                    target.pwip_kw=src.pwip_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sfbp=src.sfbp, 
                    target.sfbp_ref_compnr=src.sfbp_ref_compnr, 
                    target.stbp=src.stbp, 
                    target.stbp_ref_compnr=src.stbp_ref_compnr, 
                    target.swhu=src.swhu, 
                    target.swhu_kw=src.swhu_kw, 
                    target.timestamp=src.timestamp, 
                    target.typw=src.typw, 
                    target.typw_kw=src.typw_kw, 
                    target.username=src.username, 
                    target.wmsc=src.wmsc, 
                    target.wmsc_kw=src.wmsc_kw, 
                    target.xsbp=src.xsbp, 
                    target.xsbp_ref_compnr=src.xsbp_ref_compnr, 
                    target.xsit=src.xsit, 
                    target.xsit_kw=src.xsit_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bpid, cadr, cadr_ref_compnr, casi, casi_ref_compnr, cdf_updt, clan, clan_ref_compnr, comp, compnr, cprj, cprj_ref_compnr, cwar, cwar_eunt, deleted, dsca, duns, eunt_rcmp, imbp, imbp_ref_compnr, imgt, imgt_kw, inep, inep_kw, mesc, mesc_kw, ofbp, ofbp_ref_compnr, otbp, otbp_ref_compnr, pwip, pwip_kw, sequencenumber, sfbp, sfbp_ref_compnr, stbp, stbp_ref_compnr, swhu, swhu_kw, timestamp, typw, typw_kw, username, wmsc, wmsc_kw, xsbp, xsbp_ref_compnr, xsit, xsit_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bpid, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.casi, 
                    src.casi_ref_compnr, 
                    src.cdf_updt, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cwar, 
                    src.cwar_eunt, 
                    src.deleted, 
                    src.dsca, 
                    src.duns, 
                    src.eunt_rcmp, 
                    src.imbp, 
                    src.imbp_ref_compnr, 
                    src.imgt, 
                    src.imgt_kw, 
                    src.inep, 
                    src.inep_kw, 
                    src.mesc, 
                    src.mesc_kw, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.pwip, 
                    src.pwip_kw, 
                    src.sequencenumber, 
                    src.sfbp, 
                    src.sfbp_ref_compnr, 
                    src.stbp, 
                    src.stbp_ref_compnr, 
                    src.swhu, 
                    src.swhu_kw, 
                    src.timestamp, 
                    src.typw, 
                    src.typw_kw, 
                    src.username, 
                    src.wmsc, 
                    src.wmsc_kw, 
                    src.xsbp, 
                    src.xsbp_ref_compnr, 
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