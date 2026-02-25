create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCEMM170()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCEMM170_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCEMM170 as target using (SELECT * FROM (SELECT 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.clcu, 
            strm.clcu_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.csys, 
            strm.csys_kw, 
            strm.ctya, 
            strm.ctya_kw, 
            strm.ctyb, 
            strm.ctyb_kw, 
            strm.ctyc, 
            strm.ctyc_kw, 
            strm.ctyp, 
            strm.ctyp_kw, 
            strm.dcur_comp, 
            strm.deleted, 
            strm.desc, 
            strm.erub, 
            strm.erub_ref_compnr, 
            strm.eruc, 
            strm.eruc_ref_compnr, 
            strm.euro, 
            strm.euro_ref_compnr, 
            strm.exeu, 
            strm.exex, 
            strm.expu, 
            strm.exsa, 
            strm.fcua, 
            strm.fcua_ref_compnr, 
            strm.fcub, 
            strm.fcub_ref_compnr, 
            strm.fcuc, 
            strm.fcuc_ref_compnr, 
            strm.lcur, 
            strm.lcur_ref_compnr, 
            strm.ract, 
            strm.ract_ref_compnr, 
            strm.rdub, 
            strm.rdub_kw, 
            strm.rduc, 
            strm.rduc_kw, 
            strm.sequencenumber, 
            strm.taxo_rcmp, 
            strm.timestamp, 
            strm.tmub, 
            strm.tmub_kw, 
            strm.tmuc, 
            strm.tmuc_kw, 
            strm.tzid, 
            strm.tzid_ref_compnr, 
            strm.umfc, 
            strm.umfc_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.comp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCEMM170 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.clcu=src.clcu, 
                    target.clcu_ref_compnr=src.clcu_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.csys=src.csys, 
                    target.csys_kw=src.csys_kw, 
                    target.ctya=src.ctya, 
                    target.ctya_kw=src.ctya_kw, 
                    target.ctyb=src.ctyb, 
                    target.ctyb_kw=src.ctyb_kw, 
                    target.ctyc=src.ctyc, 
                    target.ctyc_kw=src.ctyc_kw, 
                    target.ctyp=src.ctyp, 
                    target.ctyp_kw=src.ctyp_kw, 
                    target.dcur_comp=src.dcur_comp, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.erub=src.erub, 
                    target.erub_ref_compnr=src.erub_ref_compnr, 
                    target.eruc=src.eruc, 
                    target.eruc_ref_compnr=src.eruc_ref_compnr, 
                    target.euro=src.euro, 
                    target.euro_ref_compnr=src.euro_ref_compnr, 
                    target.exeu=src.exeu, 
                    target.exex=src.exex, 
                    target.expu=src.expu, 
                    target.exsa=src.exsa, 
                    target.fcua=src.fcua, 
                    target.fcua_ref_compnr=src.fcua_ref_compnr, 
                    target.fcub=src.fcub, 
                    target.fcub_ref_compnr=src.fcub_ref_compnr, 
                    target.fcuc=src.fcuc, 
                    target.fcuc_ref_compnr=src.fcuc_ref_compnr, 
                    target.lcur=src.lcur, 
                    target.lcur_ref_compnr=src.lcur_ref_compnr, 
                    target.ract=src.ract, 
                    target.ract_ref_compnr=src.ract_ref_compnr, 
                    target.rdub=src.rdub, 
                    target.rdub_kw=src.rdub_kw, 
                    target.rduc=src.rduc, 
                    target.rduc_kw=src.rduc_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.taxo_rcmp=src.taxo_rcmp, 
                    target.timestamp=src.timestamp, 
                    target.tmub=src.tmub, 
                    target.tmub_kw=src.tmub_kw, 
                    target.tmuc=src.tmuc, 
                    target.tmuc_kw=src.tmuc_kw, 
                    target.tzid=src.tzid, 
                    target.tzid_ref_compnr=src.tzid_ref_compnr, 
                    target.umfc=src.umfc, 
                    target.umfc_kw=src.umfc_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ccal, 
                    ccal_ref_compnr, 
                    clcu, 
                    clcu_ref_compnr, 
                    comp, 
                    compnr, 
                    csys, 
                    csys_kw, 
                    ctya, 
                    ctya_kw, 
                    ctyb, 
                    ctyb_kw, 
                    ctyc, 
                    ctyc_kw, 
                    ctyp, 
                    ctyp_kw, 
                    dcur_comp, 
                    deleted, 
                    desc, 
                    erub, 
                    erub_ref_compnr, 
                    eruc, 
                    eruc_ref_compnr, 
                    euro, 
                    euro_ref_compnr, 
                    exeu, 
                    exex, 
                    expu, 
                    exsa, 
                    fcua, 
                    fcua_ref_compnr, 
                    fcub, 
                    fcub_ref_compnr, 
                    fcuc, 
                    fcuc_ref_compnr, 
                    lcur, 
                    lcur_ref_compnr, 
                    ract, 
                    ract_ref_compnr, 
                    rdub, 
                    rdub_kw, 
                    rduc, 
                    rduc_kw, 
                    sequencenumber, 
                    taxo_rcmp, 
                    timestamp, 
                    tmub, 
                    tmub_kw, 
                    tmuc, 
                    tmuc_kw, 
                    tzid, 
                    tzid_ref_compnr, 
                    umfc, 
                    umfc_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.clcu, 
                    src.clcu_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.csys, 
                    src.csys_kw, 
                    src.ctya, 
                    src.ctya_kw, 
                    src.ctyb, 
                    src.ctyb_kw, 
                    src.ctyc, 
                    src.ctyc_kw, 
                    src.ctyp, 
                    src.ctyp_kw, 
                    src.dcur_comp, 
                    src.deleted, 
                    src.desc, 
                    src.erub, 
                    src.erub_ref_compnr, 
                    src.eruc, 
                    src.eruc_ref_compnr, 
                    src.euro, 
                    src.euro_ref_compnr, 
                    src.exeu, 
                    src.exex, 
                    src.expu, 
                    src.exsa, 
                    src.fcua, 
                    src.fcua_ref_compnr, 
                    src.fcub, 
                    src.fcub_ref_compnr, 
                    src.fcuc, 
                    src.fcuc_ref_compnr, 
                    src.lcur, 
                    src.lcur_ref_compnr, 
                    src.ract, 
                    src.ract_ref_compnr, 
                    src.rdub, 
                    src.rdub_kw, 
                    src.rduc, 
                    src.rduc_kw, 
                    src.sequencenumber, 
                    src.taxo_rcmp, 
                    src.timestamp, 
                    src.tmub, 
                    src.tmub_kw, 
                    src.tmuc, 
                    src.tmuc_kw, 
                    src.tzid, 
                    src.tzid_ref_compnr, 
                    src.umfc, 
                    src.umfc_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCEMM170_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.clcu, 
            strm.clcu_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.csys, 
            strm.csys_kw, 
            strm.ctya, 
            strm.ctya_kw, 
            strm.ctyb, 
            strm.ctyb_kw, 
            strm.ctyc, 
            strm.ctyc_kw, 
            strm.ctyp, 
            strm.ctyp_kw, 
            strm.dcur_comp, 
            strm.deleted, 
            strm.desc, 
            strm.erub, 
            strm.erub_ref_compnr, 
            strm.eruc, 
            strm.eruc_ref_compnr, 
            strm.euro, 
            strm.euro_ref_compnr, 
            strm.exeu, 
            strm.exex, 
            strm.expu, 
            strm.exsa, 
            strm.fcua, 
            strm.fcua_ref_compnr, 
            strm.fcub, 
            strm.fcub_ref_compnr, 
            strm.fcuc, 
            strm.fcuc_ref_compnr, 
            strm.lcur, 
            strm.lcur_ref_compnr, 
            strm.ract, 
            strm.ract_ref_compnr, 
            strm.rdub, 
            strm.rdub_kw, 
            strm.rduc, 
            strm.rduc_kw, 
            strm.sequencenumber, 
            strm.taxo_rcmp, 
            strm.timestamp, 
            strm.tmub, 
            strm.tmub_kw, 
            strm.tmuc, 
            strm.tmuc_kw, 
            strm.tzid, 
            strm.tzid_ref_compnr, 
            strm.umfc, 
            strm.umfc_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.comp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCEMM170 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) 
                when matched then update set
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.clcu=src.clcu, 
                    target.clcu_ref_compnr=src.clcu_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.csys=src.csys, 
                    target.csys_kw=src.csys_kw, 
                    target.ctya=src.ctya, 
                    target.ctya_kw=src.ctya_kw, 
                    target.ctyb=src.ctyb, 
                    target.ctyb_kw=src.ctyb_kw, 
                    target.ctyc=src.ctyc, 
                    target.ctyc_kw=src.ctyc_kw, 
                    target.ctyp=src.ctyp, 
                    target.ctyp_kw=src.ctyp_kw, 
                    target.dcur_comp=src.dcur_comp, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.erub=src.erub, 
                    target.erub_ref_compnr=src.erub_ref_compnr, 
                    target.eruc=src.eruc, 
                    target.eruc_ref_compnr=src.eruc_ref_compnr, 
                    target.euro=src.euro, 
                    target.euro_ref_compnr=src.euro_ref_compnr, 
                    target.exeu=src.exeu, 
                    target.exex=src.exex, 
                    target.expu=src.expu, 
                    target.exsa=src.exsa, 
                    target.fcua=src.fcua, 
                    target.fcua_ref_compnr=src.fcua_ref_compnr, 
                    target.fcub=src.fcub, 
                    target.fcub_ref_compnr=src.fcub_ref_compnr, 
                    target.fcuc=src.fcuc, 
                    target.fcuc_ref_compnr=src.fcuc_ref_compnr, 
                    target.lcur=src.lcur, 
                    target.lcur_ref_compnr=src.lcur_ref_compnr, 
                    target.ract=src.ract, 
                    target.ract_ref_compnr=src.ract_ref_compnr, 
                    target.rdub=src.rdub, 
                    target.rdub_kw=src.rdub_kw, 
                    target.rduc=src.rduc, 
                    target.rduc_kw=src.rduc_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.taxo_rcmp=src.taxo_rcmp, 
                    target.timestamp=src.timestamp, 
                    target.tmub=src.tmub, 
                    target.tmub_kw=src.tmub_kw, 
                    target.tmuc=src.tmuc, 
                    target.tmuc_kw=src.tmuc_kw, 
                    target.tzid=src.tzid, 
                    target.tzid_ref_compnr=src.tzid_ref_compnr, 
                    target.umfc=src.umfc, 
                    target.umfc_kw=src.umfc_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ccal, ccal_ref_compnr, clcu, clcu_ref_compnr, comp, compnr, csys, csys_kw, ctya, ctya_kw, ctyb, ctyb_kw, ctyc, ctyc_kw, ctyp, ctyp_kw, dcur_comp, deleted, desc, erub, erub_ref_compnr, eruc, eruc_ref_compnr, euro, euro_ref_compnr, exeu, exex, expu, exsa, fcua, fcua_ref_compnr, fcub, fcub_ref_compnr, fcuc, fcuc_ref_compnr, lcur, lcur_ref_compnr, ract, ract_ref_compnr, rdub, rdub_kw, rduc, rduc_kw, sequencenumber, taxo_rcmp, timestamp, tmub, tmub_kw, tmuc, tmuc_kw, tzid, tzid_ref_compnr, umfc, umfc_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.clcu, 
                    src.clcu_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.csys, 
                    src.csys_kw, 
                    src.ctya, 
                    src.ctya_kw, 
                    src.ctyb, 
                    src.ctyb_kw, 
                    src.ctyc, 
                    src.ctyc_kw, 
                    src.ctyp, 
                    src.ctyp_kw, 
                    src.dcur_comp, 
                    src.deleted, 
                    src.desc, 
                    src.erub, 
                    src.erub_ref_compnr, 
                    src.eruc, 
                    src.eruc_ref_compnr, 
                    src.euro, 
                    src.euro_ref_compnr, 
                    src.exeu, 
                    src.exex, 
                    src.expu, 
                    src.exsa, 
                    src.fcua, 
                    src.fcua_ref_compnr, 
                    src.fcub, 
                    src.fcub_ref_compnr, 
                    src.fcuc, 
                    src.fcuc_ref_compnr, 
                    src.lcur, 
                    src.lcur_ref_compnr, 
                    src.ract, 
                    src.ract_ref_compnr, 
                    src.rdub, 
                    src.rdub_kw, 
                    src.rduc, 
                    src.rduc_kw, 
                    src.sequencenumber, 
                    src.taxo_rcmp, 
                    src.timestamp, 
                    src.tmub, 
                    src.tmub_kw, 
                    src.tmuc, 
                    src.tmuc_kw, 
                    src.tzid, 
                    src.tzid_ref_compnr, 
                    src.umfc, 
                    src.umfc_kw, 
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