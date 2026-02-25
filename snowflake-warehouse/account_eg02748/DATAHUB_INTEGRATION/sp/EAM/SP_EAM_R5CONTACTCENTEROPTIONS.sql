create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CONTACTCENTEROPTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CONTACTCENTEROPTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CONTACTCENTEROPTIONS as target using (SELECT * FROM (SELECT 
            strm.COP_CHKDUPLICATEOPENREQ, 
            strm.COP_CHKRECURRINGCLOSEDREQ, 
            strm.COP_CLOSEABLE, 
            strm.COP_COPYCUSTOMERINFO, 
            strm.COP_DEFCONTACTSOURCE, 
            strm.COP_DEFFINDBY, 
            strm.COP_DEFFOLLOWUPSTATUS, 
            strm.COP_DEFOPENSTATUS, 
            strm.COP_DEFORGUSAGE, 
            strm.COP_DEFPORTALSOURCE, 
            strm.COP_DEFPORTALSTATUS, 
            strm.COP_DEFPORTALTYPE, 
            strm.COP_DEFTYPE, 
            strm.COP_DEFWOORG, 
            strm.COP_DEFWOSTATUS, 
            strm.COP_DEPTSTRUCTUREUSAGE, 
            strm.COP_EVENTTYPEFILTER, 
            strm.COP_HIGHLIGHTMAP, 
            strm.COP_IDEFEATURE, 
            strm.COP_INCLUDEPMWORKORDERS, 
            strm.COP_KBRESULTSPERPAGE, 
            strm.COP_LASTSAVED, 
            strm.COP_MATCHSC, 
            strm.COP_MATCHSPC, 
            strm.COP_MINPENALTY, 
            strm.COP_NEARADDRESS, 
            strm.COP_NEWCUSTOMERALLOWED, 
            strm.COP_ORG, 
            strm.COP_RECURRINGREQLOOKBACKDAYS, 
            strm.COP_SCHEDULEWO, 
            strm.COP_SHOWCHILDREN, 
            strm.COP_SHOWPROVIDER, 
            strm.COP_SHOWSERVICECATEGORY, 
            strm.COP_SHOWSPC, 
            strm.COP_SHOWSUPPLIER, 
            strm.COP_SPCVALIDATION, 
            strm.COP_TOPTENLOOKBACK, 
            strm.COP_UPDATECOUNT, 
            strm.COP_UPDATED, 
            strm.COP_UPDATEDBY, 
            strm.COP_WOCLOSEDAYS, 
            strm.COP_WODUPLICATECHECK, 
            strm.COP_WOHEADEROBJONLY, 
            strm.COP_WOOPENDAYS, 
            strm.COP_WOSTATUSES, 
            strm.COP_WOTYPES, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COP_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONTACTCENTEROPTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COP_ORG=src.COP_ORG) OR (target.COP_ORG IS NULL AND src.COP_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.COP_CHKDUPLICATEOPENREQ=src.COP_CHKDUPLICATEOPENREQ, 
                    target.COP_CHKRECURRINGCLOSEDREQ=src.COP_CHKRECURRINGCLOSEDREQ, 
                    target.COP_CLOSEABLE=src.COP_CLOSEABLE, 
                    target.COP_COPYCUSTOMERINFO=src.COP_COPYCUSTOMERINFO, 
                    target.COP_DEFCONTACTSOURCE=src.COP_DEFCONTACTSOURCE, 
                    target.COP_DEFFINDBY=src.COP_DEFFINDBY, 
                    target.COP_DEFFOLLOWUPSTATUS=src.COP_DEFFOLLOWUPSTATUS, 
                    target.COP_DEFOPENSTATUS=src.COP_DEFOPENSTATUS, 
                    target.COP_DEFORGUSAGE=src.COP_DEFORGUSAGE, 
                    target.COP_DEFPORTALSOURCE=src.COP_DEFPORTALSOURCE, 
                    target.COP_DEFPORTALSTATUS=src.COP_DEFPORTALSTATUS, 
                    target.COP_DEFPORTALTYPE=src.COP_DEFPORTALTYPE, 
                    target.COP_DEFTYPE=src.COP_DEFTYPE, 
                    target.COP_DEFWOORG=src.COP_DEFWOORG, 
                    target.COP_DEFWOSTATUS=src.COP_DEFWOSTATUS, 
                    target.COP_DEPTSTRUCTUREUSAGE=src.COP_DEPTSTRUCTUREUSAGE, 
                    target.COP_EVENTTYPEFILTER=src.COP_EVENTTYPEFILTER, 
                    target.COP_HIGHLIGHTMAP=src.COP_HIGHLIGHTMAP, 
                    target.COP_IDEFEATURE=src.COP_IDEFEATURE, 
                    target.COP_INCLUDEPMWORKORDERS=src.COP_INCLUDEPMWORKORDERS, 
                    target.COP_KBRESULTSPERPAGE=src.COP_KBRESULTSPERPAGE, 
                    target.COP_LASTSAVED=src.COP_LASTSAVED, 
                    target.COP_MATCHSC=src.COP_MATCHSC, 
                    target.COP_MATCHSPC=src.COP_MATCHSPC, 
                    target.COP_MINPENALTY=src.COP_MINPENALTY, 
                    target.COP_NEARADDRESS=src.COP_NEARADDRESS, 
                    target.COP_NEWCUSTOMERALLOWED=src.COP_NEWCUSTOMERALLOWED, 
                    target.COP_ORG=src.COP_ORG, 
                    target.COP_RECURRINGREQLOOKBACKDAYS=src.COP_RECURRINGREQLOOKBACKDAYS, 
                    target.COP_SCHEDULEWO=src.COP_SCHEDULEWO, 
                    target.COP_SHOWCHILDREN=src.COP_SHOWCHILDREN, 
                    target.COP_SHOWPROVIDER=src.COP_SHOWPROVIDER, 
                    target.COP_SHOWSERVICECATEGORY=src.COP_SHOWSERVICECATEGORY, 
                    target.COP_SHOWSPC=src.COP_SHOWSPC, 
                    target.COP_SHOWSUPPLIER=src.COP_SHOWSUPPLIER, 
                    target.COP_SPCVALIDATION=src.COP_SPCVALIDATION, 
                    target.COP_TOPTENLOOKBACK=src.COP_TOPTENLOOKBACK, 
                    target.COP_UPDATECOUNT=src.COP_UPDATECOUNT, 
                    target.COP_UPDATED=src.COP_UPDATED, 
                    target.COP_UPDATEDBY=src.COP_UPDATEDBY, 
                    target.COP_WOCLOSEDAYS=src.COP_WOCLOSEDAYS, 
                    target.COP_WODUPLICATECHECK=src.COP_WODUPLICATECHECK, 
                    target.COP_WOHEADEROBJONLY=src.COP_WOHEADEROBJONLY, 
                    target.COP_WOOPENDAYS=src.COP_WOOPENDAYS, 
                    target.COP_WOSTATUSES=src.COP_WOSTATUSES, 
                    target.COP_WOTYPES=src.COP_WOTYPES, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    COP_CHKDUPLICATEOPENREQ, 
                    COP_CHKRECURRINGCLOSEDREQ, 
                    COP_CLOSEABLE, 
                    COP_COPYCUSTOMERINFO, 
                    COP_DEFCONTACTSOURCE, 
                    COP_DEFFINDBY, 
                    COP_DEFFOLLOWUPSTATUS, 
                    COP_DEFOPENSTATUS, 
                    COP_DEFORGUSAGE, 
                    COP_DEFPORTALSOURCE, 
                    COP_DEFPORTALSTATUS, 
                    COP_DEFPORTALTYPE, 
                    COP_DEFTYPE, 
                    COP_DEFWOORG, 
                    COP_DEFWOSTATUS, 
                    COP_DEPTSTRUCTUREUSAGE, 
                    COP_EVENTTYPEFILTER, 
                    COP_HIGHLIGHTMAP, 
                    COP_IDEFEATURE, 
                    COP_INCLUDEPMWORKORDERS, 
                    COP_KBRESULTSPERPAGE, 
                    COP_LASTSAVED, 
                    COP_MATCHSC, 
                    COP_MATCHSPC, 
                    COP_MINPENALTY, 
                    COP_NEARADDRESS, 
                    COP_NEWCUSTOMERALLOWED, 
                    COP_ORG, 
                    COP_RECURRINGREQLOOKBACKDAYS, 
                    COP_SCHEDULEWO, 
                    COP_SHOWCHILDREN, 
                    COP_SHOWPROVIDER, 
                    COP_SHOWSERVICECATEGORY, 
                    COP_SHOWSPC, 
                    COP_SHOWSUPPLIER, 
                    COP_SPCVALIDATION, 
                    COP_TOPTENLOOKBACK, 
                    COP_UPDATECOUNT, 
                    COP_UPDATED, 
                    COP_UPDATEDBY, 
                    COP_WOCLOSEDAYS, 
                    COP_WODUPLICATECHECK, 
                    COP_WOHEADEROBJONLY, 
                    COP_WOOPENDAYS, 
                    COP_WOSTATUSES, 
                    COP_WOTYPES, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.COP_CHKDUPLICATEOPENREQ, 
                    src.COP_CHKRECURRINGCLOSEDREQ, 
                    src.COP_CLOSEABLE, 
                    src.COP_COPYCUSTOMERINFO, 
                    src.COP_DEFCONTACTSOURCE, 
                    src.COP_DEFFINDBY, 
                    src.COP_DEFFOLLOWUPSTATUS, 
                    src.COP_DEFOPENSTATUS, 
                    src.COP_DEFORGUSAGE, 
                    src.COP_DEFPORTALSOURCE, 
                    src.COP_DEFPORTALSTATUS, 
                    src.COP_DEFPORTALTYPE, 
                    src.COP_DEFTYPE, 
                    src.COP_DEFWOORG, 
                    src.COP_DEFWOSTATUS, 
                    src.COP_DEPTSTRUCTUREUSAGE, 
                    src.COP_EVENTTYPEFILTER, 
                    src.COP_HIGHLIGHTMAP, 
                    src.COP_IDEFEATURE, 
                    src.COP_INCLUDEPMWORKORDERS, 
                    src.COP_KBRESULTSPERPAGE, 
                    src.COP_LASTSAVED, 
                    src.COP_MATCHSC, 
                    src.COP_MATCHSPC, 
                    src.COP_MINPENALTY, 
                    src.COP_NEARADDRESS, 
                    src.COP_NEWCUSTOMERALLOWED, 
                    src.COP_ORG, 
                    src.COP_RECURRINGREQLOOKBACKDAYS, 
                    src.COP_SCHEDULEWO, 
                    src.COP_SHOWCHILDREN, 
                    src.COP_SHOWPROVIDER, 
                    src.COP_SHOWSERVICECATEGORY, 
                    src.COP_SHOWSPC, 
                    src.COP_SHOWSUPPLIER, 
                    src.COP_SPCVALIDATION, 
                    src.COP_TOPTENLOOKBACK, 
                    src.COP_UPDATECOUNT, 
                    src.COP_UPDATED, 
                    src.COP_UPDATEDBY, 
                    src.COP_WOCLOSEDAYS, 
                    src.COP_WODUPLICATECHECK, 
                    src.COP_WOHEADEROBJONLY, 
                    src.COP_WOOPENDAYS, 
                    src.COP_WOSTATUSES, 
                    src.COP_WOTYPES, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CONTACTCENTEROPTIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.COP_CHKDUPLICATEOPENREQ, 
            strm.COP_CHKRECURRINGCLOSEDREQ, 
            strm.COP_CLOSEABLE, 
            strm.COP_COPYCUSTOMERINFO, 
            strm.COP_DEFCONTACTSOURCE, 
            strm.COP_DEFFINDBY, 
            strm.COP_DEFFOLLOWUPSTATUS, 
            strm.COP_DEFOPENSTATUS, 
            strm.COP_DEFORGUSAGE, 
            strm.COP_DEFPORTALSOURCE, 
            strm.COP_DEFPORTALSTATUS, 
            strm.COP_DEFPORTALTYPE, 
            strm.COP_DEFTYPE, 
            strm.COP_DEFWOORG, 
            strm.COP_DEFWOSTATUS, 
            strm.COP_DEPTSTRUCTUREUSAGE, 
            strm.COP_EVENTTYPEFILTER, 
            strm.COP_HIGHLIGHTMAP, 
            strm.COP_IDEFEATURE, 
            strm.COP_INCLUDEPMWORKORDERS, 
            strm.COP_KBRESULTSPERPAGE, 
            strm.COP_LASTSAVED, 
            strm.COP_MATCHSC, 
            strm.COP_MATCHSPC, 
            strm.COP_MINPENALTY, 
            strm.COP_NEARADDRESS, 
            strm.COP_NEWCUSTOMERALLOWED, 
            strm.COP_ORG, 
            strm.COP_RECURRINGREQLOOKBACKDAYS, 
            strm.COP_SCHEDULEWO, 
            strm.COP_SHOWCHILDREN, 
            strm.COP_SHOWPROVIDER, 
            strm.COP_SHOWSERVICECATEGORY, 
            strm.COP_SHOWSPC, 
            strm.COP_SHOWSUPPLIER, 
            strm.COP_SPCVALIDATION, 
            strm.COP_TOPTENLOOKBACK, 
            strm.COP_UPDATECOUNT, 
            strm.COP_UPDATED, 
            strm.COP_UPDATEDBY, 
            strm.COP_WOCLOSEDAYS, 
            strm.COP_WODUPLICATECHECK, 
            strm.COP_WOHEADEROBJONLY, 
            strm.COP_WOOPENDAYS, 
            strm.COP_WOSTATUSES, 
            strm.COP_WOTYPES, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COP_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONTACTCENTEROPTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COP_ORG=src.COP_ORG) OR (target.COP_ORG IS NULL AND src.COP_ORG IS NULL)) 
                when matched then update set
                    target.COP_CHKDUPLICATEOPENREQ=src.COP_CHKDUPLICATEOPENREQ, 
                    target.COP_CHKRECURRINGCLOSEDREQ=src.COP_CHKRECURRINGCLOSEDREQ, 
                    target.COP_CLOSEABLE=src.COP_CLOSEABLE, 
                    target.COP_COPYCUSTOMERINFO=src.COP_COPYCUSTOMERINFO, 
                    target.COP_DEFCONTACTSOURCE=src.COP_DEFCONTACTSOURCE, 
                    target.COP_DEFFINDBY=src.COP_DEFFINDBY, 
                    target.COP_DEFFOLLOWUPSTATUS=src.COP_DEFFOLLOWUPSTATUS, 
                    target.COP_DEFOPENSTATUS=src.COP_DEFOPENSTATUS, 
                    target.COP_DEFORGUSAGE=src.COP_DEFORGUSAGE, 
                    target.COP_DEFPORTALSOURCE=src.COP_DEFPORTALSOURCE, 
                    target.COP_DEFPORTALSTATUS=src.COP_DEFPORTALSTATUS, 
                    target.COP_DEFPORTALTYPE=src.COP_DEFPORTALTYPE, 
                    target.COP_DEFTYPE=src.COP_DEFTYPE, 
                    target.COP_DEFWOORG=src.COP_DEFWOORG, 
                    target.COP_DEFWOSTATUS=src.COP_DEFWOSTATUS, 
                    target.COP_DEPTSTRUCTUREUSAGE=src.COP_DEPTSTRUCTUREUSAGE, 
                    target.COP_EVENTTYPEFILTER=src.COP_EVENTTYPEFILTER, 
                    target.COP_HIGHLIGHTMAP=src.COP_HIGHLIGHTMAP, 
                    target.COP_IDEFEATURE=src.COP_IDEFEATURE, 
                    target.COP_INCLUDEPMWORKORDERS=src.COP_INCLUDEPMWORKORDERS, 
                    target.COP_KBRESULTSPERPAGE=src.COP_KBRESULTSPERPAGE, 
                    target.COP_LASTSAVED=src.COP_LASTSAVED, 
                    target.COP_MATCHSC=src.COP_MATCHSC, 
                    target.COP_MATCHSPC=src.COP_MATCHSPC, 
                    target.COP_MINPENALTY=src.COP_MINPENALTY, 
                    target.COP_NEARADDRESS=src.COP_NEARADDRESS, 
                    target.COP_NEWCUSTOMERALLOWED=src.COP_NEWCUSTOMERALLOWED, 
                    target.COP_ORG=src.COP_ORG, 
                    target.COP_RECURRINGREQLOOKBACKDAYS=src.COP_RECURRINGREQLOOKBACKDAYS, 
                    target.COP_SCHEDULEWO=src.COP_SCHEDULEWO, 
                    target.COP_SHOWCHILDREN=src.COP_SHOWCHILDREN, 
                    target.COP_SHOWPROVIDER=src.COP_SHOWPROVIDER, 
                    target.COP_SHOWSERVICECATEGORY=src.COP_SHOWSERVICECATEGORY, 
                    target.COP_SHOWSPC=src.COP_SHOWSPC, 
                    target.COP_SHOWSUPPLIER=src.COP_SHOWSUPPLIER, 
                    target.COP_SPCVALIDATION=src.COP_SPCVALIDATION, 
                    target.COP_TOPTENLOOKBACK=src.COP_TOPTENLOOKBACK, 
                    target.COP_UPDATECOUNT=src.COP_UPDATECOUNT, 
                    target.COP_UPDATED=src.COP_UPDATED, 
                    target.COP_UPDATEDBY=src.COP_UPDATEDBY, 
                    target.COP_WOCLOSEDAYS=src.COP_WOCLOSEDAYS, 
                    target.COP_WODUPLICATECHECK=src.COP_WODUPLICATECHECK, 
                    target.COP_WOHEADEROBJONLY=src.COP_WOHEADEROBJONLY, 
                    target.COP_WOOPENDAYS=src.COP_WOOPENDAYS, 
                    target.COP_WOSTATUSES=src.COP_WOSTATUSES, 
                    target.COP_WOTYPES=src.COP_WOTYPES, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( COP_CHKDUPLICATEOPENREQ, COP_CHKRECURRINGCLOSEDREQ, COP_CLOSEABLE, COP_COPYCUSTOMERINFO, COP_DEFCONTACTSOURCE, COP_DEFFINDBY, COP_DEFFOLLOWUPSTATUS, COP_DEFOPENSTATUS, COP_DEFORGUSAGE, COP_DEFPORTALSOURCE, COP_DEFPORTALSTATUS, COP_DEFPORTALTYPE, COP_DEFTYPE, COP_DEFWOORG, COP_DEFWOSTATUS, COP_DEPTSTRUCTUREUSAGE, COP_EVENTTYPEFILTER, COP_HIGHLIGHTMAP, COP_IDEFEATURE, COP_INCLUDEPMWORKORDERS, COP_KBRESULTSPERPAGE, COP_LASTSAVED, COP_MATCHSC, COP_MATCHSPC, COP_MINPENALTY, COP_NEARADDRESS, COP_NEWCUSTOMERALLOWED, COP_ORG, COP_RECURRINGREQLOOKBACKDAYS, COP_SCHEDULEWO, COP_SHOWCHILDREN, COP_SHOWPROVIDER, COP_SHOWSERVICECATEGORY, COP_SHOWSPC, COP_SHOWSUPPLIER, COP_SPCVALIDATION, COP_TOPTENLOOKBACK, COP_UPDATECOUNT, COP_UPDATED, COP_UPDATEDBY, COP_WOCLOSEDAYS, COP_WODUPLICATECHECK, COP_WOHEADEROBJONLY, COP_WOOPENDAYS, COP_WOSTATUSES, COP_WOTYPES, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.COP_CHKDUPLICATEOPENREQ, 
                    src.COP_CHKRECURRINGCLOSEDREQ, 
                    src.COP_CLOSEABLE, 
                    src.COP_COPYCUSTOMERINFO, 
                    src.COP_DEFCONTACTSOURCE, 
                    src.COP_DEFFINDBY, 
                    src.COP_DEFFOLLOWUPSTATUS, 
                    src.COP_DEFOPENSTATUS, 
                    src.COP_DEFORGUSAGE, 
                    src.COP_DEFPORTALSOURCE, 
                    src.COP_DEFPORTALSTATUS, 
                    src.COP_DEFPORTALTYPE, 
                    src.COP_DEFTYPE, 
                    src.COP_DEFWOORG, 
                    src.COP_DEFWOSTATUS, 
                    src.COP_DEPTSTRUCTUREUSAGE, 
                    src.COP_EVENTTYPEFILTER, 
                    src.COP_HIGHLIGHTMAP, 
                    src.COP_IDEFEATURE, 
                    src.COP_INCLUDEPMWORKORDERS, 
                    src.COP_KBRESULTSPERPAGE, 
                    src.COP_LASTSAVED, 
                    src.COP_MATCHSC, 
                    src.COP_MATCHSPC, 
                    src.COP_MINPENALTY, 
                    src.COP_NEARADDRESS, 
                    src.COP_NEWCUSTOMERALLOWED, 
                    src.COP_ORG, 
                    src.COP_RECURRINGREQLOOKBACKDAYS, 
                    src.COP_SCHEDULEWO, 
                    src.COP_SHOWCHILDREN, 
                    src.COP_SHOWPROVIDER, 
                    src.COP_SHOWSERVICECATEGORY, 
                    src.COP_SHOWSPC, 
                    src.COP_SHOWSUPPLIER, 
                    src.COP_SPCVALIDATION, 
                    src.COP_TOPTENLOOKBACK, 
                    src.COP_UPDATECOUNT, 
                    src.COP_UPDATED, 
                    src.COP_UPDATEDBY, 
                    src.COP_WOCLOSEDAYS, 
                    src.COP_WODUPLICATECHECK, 
                    src.COP_WOHEADEROBJONLY, 
                    src.COP_WOOPENDAYS, 
                    src.COP_WOSTATUSES, 
                    src.COP_WOTYPES, 
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