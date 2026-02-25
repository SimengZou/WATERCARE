create or replace procedure DATAHUB_INTEGRATION.SP_EAM_U5CLAIMCONFIG()
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
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_U5CLAIMCONFIG_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_U5CLAIMCONFIG as target using (SELECT * FROM (SELECT 
            strm.CLC_CHILDWORATEAPP, 
            strm.CLC_CONTORDERTYPE, 
            strm.CLC_CONTRACTOR, 
            strm.CLC_COSTITEM, 
            strm.CLC_CREATOR, 
            strm.CLC_EXCLUDEPOFROMLN, 
            strm.CLC_EXCLUDEWOFROMLN, 
            strm.CLC_LNCOMPANY, 
            strm.CLC_NONCONTORDERTYPE, 
            strm.CLC_ORDERSERIES, 
            strm.CLC_ORG, 
            strm.CLC_PMWORATEAPP, 
            strm.CLC_PROJECTACTIVITY, 
            strm.CLC_PROJECTNUMBER, 
            strm.CLC_PURCHASEOFFICE, 
            strm.CLC_REQUESTOR, 
            strm.CLC_REQUESTOR2, 
            strm.CLC_SPLITINTERCOMPANYCOSTS, 
            strm.CLC_STCR_DIM3, 
            strm.CLC_STCR_DIM4, 
            strm.CLC_STGCOSTCODE, 
            strm.CLC_STORE, 
            strm.CLC_SUPPLIER, 
            strm.CLC_TRANS_FLAG, 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLC_ORG,
            strm.CLC_CONTRACTOR ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMCONFIG as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLC_ORG=src.CLC_ORG) OR (target.CLC_ORG IS NULL AND src.CLC_ORG IS NULL)) AND
            ((target.CLC_CONTRACTOR=src.CLC_CONTRACTOR) OR (target.CLC_CONTRACTOR IS NULL AND src.CLC_CONTRACTOR IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CLC_CHILDWORATEAPP=src.CLC_CHILDWORATEAPP, 
                    target.CLC_CONTORDERTYPE=src.CLC_CONTORDERTYPE, 
                    target.CLC_CONTRACTOR=src.CLC_CONTRACTOR, 
                    target.CLC_COSTITEM=src.CLC_COSTITEM, 
                    target.CLC_CREATOR=src.CLC_CREATOR, 
                    target.CLC_EXCLUDEPOFROMLN=src.CLC_EXCLUDEPOFROMLN, 
                    target.CLC_EXCLUDEWOFROMLN=src.CLC_EXCLUDEWOFROMLN, 
                    target.CLC_LNCOMPANY=src.CLC_LNCOMPANY, 
                    target.CLC_NONCONTORDERTYPE=src.CLC_NONCONTORDERTYPE, 
                    target.CLC_ORDERSERIES=src.CLC_ORDERSERIES, 
                    target.CLC_ORG=src.CLC_ORG, 
                    target.CLC_PMWORATEAPP=src.CLC_PMWORATEAPP, 
                    target.CLC_PROJECTACTIVITY=src.CLC_PROJECTACTIVITY, 
                    target.CLC_PROJECTNUMBER=src.CLC_PROJECTNUMBER, 
                    target.CLC_PURCHASEOFFICE=src.CLC_PURCHASEOFFICE, 
                    target.CLC_REQUESTOR=src.CLC_REQUESTOR, 
                    target.CLC_REQUESTOR2=src.CLC_REQUESTOR2, 
                    target.CLC_SPLITINTERCOMPANYCOSTS=src.CLC_SPLITINTERCOMPANYCOSTS, 
                    target.CLC_STCR_DIM3=src.CLC_STCR_DIM3, 
                    target.CLC_STCR_DIM4=src.CLC_STCR_DIM4, 
                    target.CLC_STGCOSTCODE=src.CLC_STGCOSTCODE, 
                    target.CLC_STORE=src.CLC_STORE, 
                    target.CLC_SUPPLIER=src.CLC_SUPPLIER, 
                    target.CLC_TRANS_FLAG=src.CLC_TRANS_FLAG, 
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CLC_CHILDWORATEAPP, 
                    CLC_CONTORDERTYPE, 
                    CLC_CONTRACTOR, 
                    CLC_COSTITEM, 
                    CLC_CREATOR, 
                    CLC_EXCLUDEPOFROMLN, 
                    CLC_EXCLUDEWOFROMLN, 
                    CLC_LNCOMPANY, 
                    CLC_NONCONTORDERTYPE, 
                    CLC_ORDERSERIES, 
                    CLC_ORG, 
                    CLC_PMWORATEAPP, 
                    CLC_PROJECTACTIVITY, 
                    CLC_PROJECTNUMBER, 
                    CLC_PURCHASEOFFICE, 
                    CLC_REQUESTOR, 
                    CLC_REQUESTOR2, 
                    CLC_SPLITINTERCOMPANYCOSTS, 
                    CLC_STCR_DIM3, 
                    CLC_STCR_DIM4, 
                    CLC_STGCOSTCODE, 
                    CLC_STORE, 
                    CLC_SUPPLIER, 
                    CLC_TRANS_FLAG, 
                    CREATED, 
                    CREATEDBY, 
                    LASTSAVED, 
                    UPDATECOUNT, 
                    UPDATED, 
                    UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CLC_CHILDWORATEAPP, 
                    src.CLC_CONTORDERTYPE, 
                    src.CLC_CONTRACTOR, 
                    src.CLC_COSTITEM, 
                    src.CLC_CREATOR, 
                    src.CLC_EXCLUDEPOFROMLN, 
                    src.CLC_EXCLUDEWOFROMLN, 
                    src.CLC_LNCOMPANY, 
                    src.CLC_NONCONTORDERTYPE, 
                    src.CLC_ORDERSERIES, 
                    src.CLC_ORG, 
                    src.CLC_PMWORATEAPP, 
                    src.CLC_PROJECTACTIVITY, 
                    src.CLC_PROJECTNUMBER, 
                    src.CLC_PURCHASEOFFICE, 
                    src.CLC_REQUESTOR, 
                    src.CLC_REQUESTOR2, 
                    src.CLC_SPLITINTERCOMPANYCOSTS, 
                    src.CLC_STCR_DIM3, 
                    src.CLC_STCR_DIM4, 
                    src.CLC_STGCOSTCODE, 
                    src.CLC_STORE, 
                    src.CLC_SUPPLIER, 
                    src.CLC_TRANS_FLAG, 
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_U5CLAIMCONFIG_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CLC_CHILDWORATEAPP, 
            strm.CLC_CONTORDERTYPE, 
            strm.CLC_CONTRACTOR, 
            strm.CLC_COSTITEM, 
            strm.CLC_CREATOR, 
            strm.CLC_EXCLUDEPOFROMLN, 
            strm.CLC_EXCLUDEWOFROMLN, 
            strm.CLC_LNCOMPANY, 
            strm.CLC_NONCONTORDERTYPE, 
            strm.CLC_ORDERSERIES, 
            strm.CLC_ORG, 
            strm.CLC_PMWORATEAPP, 
            strm.CLC_PROJECTACTIVITY, 
            strm.CLC_PROJECTNUMBER, 
            strm.CLC_PURCHASEOFFICE, 
            strm.CLC_REQUESTOR, 
            strm.CLC_REQUESTOR2, 
            strm.CLC_SPLITINTERCOMPANYCOSTS, 
            strm.CLC_STCR_DIM3, 
            strm.CLC_STCR_DIM4, 
            strm.CLC_STGCOSTCODE, 
            strm.CLC_STORE, 
            strm.CLC_SUPPLIER, 
            strm.CLC_TRANS_FLAG, 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLC_ORG,
            strm.CLC_CONTRACTOR ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMCONFIG as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLC_ORG=src.CLC_ORG) OR (target.CLC_ORG IS NULL AND src.CLC_ORG IS NULL)) AND
            ((target.CLC_CONTRACTOR=src.CLC_CONTRACTOR) OR (target.CLC_CONTRACTOR IS NULL AND src.CLC_CONTRACTOR IS NULL)) 
                when matched then update set
                    target.CLC_CHILDWORATEAPP=src.CLC_CHILDWORATEAPP, 
                    target.CLC_CONTORDERTYPE=src.CLC_CONTORDERTYPE, 
                    target.CLC_CONTRACTOR=src.CLC_CONTRACTOR, 
                    target.CLC_COSTITEM=src.CLC_COSTITEM, 
                    target.CLC_CREATOR=src.CLC_CREATOR, 
                    target.CLC_EXCLUDEPOFROMLN=src.CLC_EXCLUDEPOFROMLN, 
                    target.CLC_EXCLUDEWOFROMLN=src.CLC_EXCLUDEWOFROMLN, 
                    target.CLC_LNCOMPANY=src.CLC_LNCOMPANY, 
                    target.CLC_NONCONTORDERTYPE=src.CLC_NONCONTORDERTYPE, 
                    target.CLC_ORDERSERIES=src.CLC_ORDERSERIES, 
                    target.CLC_ORG=src.CLC_ORG, 
                    target.CLC_PMWORATEAPP=src.CLC_PMWORATEAPP, 
                    target.CLC_PROJECTACTIVITY=src.CLC_PROJECTACTIVITY, 
                    target.CLC_PROJECTNUMBER=src.CLC_PROJECTNUMBER, 
                    target.CLC_PURCHASEOFFICE=src.CLC_PURCHASEOFFICE, 
                    target.CLC_REQUESTOR=src.CLC_REQUESTOR, 
                    target.CLC_REQUESTOR2=src.CLC_REQUESTOR2, 
                    target.CLC_SPLITINTERCOMPANYCOSTS=src.CLC_SPLITINTERCOMPANYCOSTS, 
                    target.CLC_STCR_DIM3=src.CLC_STCR_DIM3, 
                    target.CLC_STCR_DIM4=src.CLC_STCR_DIM4, 
                    target.CLC_STGCOSTCODE=src.CLC_STGCOSTCODE, 
                    target.CLC_STORE=src.CLC_STORE, 
                    target.CLC_SUPPLIER=src.CLC_SUPPLIER, 
                    target.CLC_TRANS_FLAG=src.CLC_TRANS_FLAG, 
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CLC_CHILDWORATEAPP, CLC_CONTORDERTYPE, CLC_CONTRACTOR, CLC_COSTITEM, CLC_CREATOR, CLC_EXCLUDEPOFROMLN, CLC_EXCLUDEWOFROMLN, CLC_LNCOMPANY, CLC_NONCONTORDERTYPE, CLC_ORDERSERIES, CLC_ORG, CLC_PMWORATEAPP, CLC_PROJECTACTIVITY, CLC_PROJECTNUMBER, CLC_PURCHASEOFFICE, CLC_REQUESTOR, CLC_REQUESTOR2, CLC_SPLITINTERCOMPANYCOSTS, CLC_STCR_DIM3, CLC_STCR_DIM4, CLC_STGCOSTCODE, CLC_STORE, CLC_SUPPLIER, CLC_TRANS_FLAG, CREATED, CREATEDBY, LASTSAVED, UPDATECOUNT, UPDATED, UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CLC_CHILDWORATEAPP, 
                    src.CLC_CONTORDERTYPE, 
                    src.CLC_CONTRACTOR, 
                    src.CLC_COSTITEM, 
                    src.CLC_CREATOR, 
                    src.CLC_EXCLUDEPOFROMLN, 
                    src.CLC_EXCLUDEWOFROMLN, 
                    src.CLC_LNCOMPANY, 
                    src.CLC_NONCONTORDERTYPE, 
                    src.CLC_ORDERSERIES, 
                    src.CLC_ORG, 
                    src.CLC_PMWORATEAPP, 
                    src.CLC_PROJECTACTIVITY, 
                    src.CLC_PROJECTNUMBER, 
                    src.CLC_PURCHASEOFFICE, 
                    src.CLC_REQUESTOR, 
                    src.CLC_REQUESTOR2, 
                    src.CLC_SPLITINTERCOMPANYCOSTS, 
                    src.CLC_STCR_DIM3, 
                    src.CLC_STCR_DIM4, 
                    src.CLC_STGCOSTCODE, 
                    src.CLC_STORE, 
                    src.CLC_SUPPLIER, 
                    src.CLC_TRANS_FLAG, 
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
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