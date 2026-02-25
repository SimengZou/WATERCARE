create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5USERLINEARPREFERENCES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5USERLINEARPREFERENCES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5USERLINEARPREFERENCES as target using (SELECT * FROM (SELECT 
            strm.ULP_CODE, 
            strm.ULP_CREATED, 
            strm.ULP_CREATEDBY, 
            strm.ULP_DEFAULT, 
            strm.ULP_DESC, 
            strm.ULP_GROUPSEGMENTS, 
            strm.ULP_HIDEROUES, 
            strm.ULP_HIDESEGMENTS, 
            strm.ULP_LASTSAVED, 
            strm.ULP_LODEFAULTCOLOR, 
            strm.ULP_LODEFAULTFILTER, 
            strm.ULP_LOINCLUDERELATED, 
            strm.ULP_LOLINEARREF, 
            strm.ULP_LOPOINTSOFINT, 
            strm.ULP_LORELATEDEQ, 
            strm.ULP_LORELATEDPART, 
            strm.ULP_LOROUTEANDSEGMENT, 
            strm.ULP_UPDATECOUNT, 
            strm.ULP_UPDATED, 
            strm.ULP_UPDATEDBY, 
            strm.ULP_USERCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ULP_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERLINEARPREFERENCES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ULP_CODE=src.ULP_CODE) OR (target.ULP_CODE IS NULL AND src.ULP_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ULP_CODE=src.ULP_CODE, 
                    target.ULP_CREATED=src.ULP_CREATED, 
                    target.ULP_CREATEDBY=src.ULP_CREATEDBY, 
                    target.ULP_DEFAULT=src.ULP_DEFAULT, 
                    target.ULP_DESC=src.ULP_DESC, 
                    target.ULP_GROUPSEGMENTS=src.ULP_GROUPSEGMENTS, 
                    target.ULP_HIDEROUES=src.ULP_HIDEROUES, 
                    target.ULP_HIDESEGMENTS=src.ULP_HIDESEGMENTS, 
                    target.ULP_LASTSAVED=src.ULP_LASTSAVED, 
                    target.ULP_LODEFAULTCOLOR=src.ULP_LODEFAULTCOLOR, 
                    target.ULP_LODEFAULTFILTER=src.ULP_LODEFAULTFILTER, 
                    target.ULP_LOINCLUDERELATED=src.ULP_LOINCLUDERELATED, 
                    target.ULP_LOLINEARREF=src.ULP_LOLINEARREF, 
                    target.ULP_LOPOINTSOFINT=src.ULP_LOPOINTSOFINT, 
                    target.ULP_LORELATEDEQ=src.ULP_LORELATEDEQ, 
                    target.ULP_LORELATEDPART=src.ULP_LORELATEDPART, 
                    target.ULP_LOROUTEANDSEGMENT=src.ULP_LOROUTEANDSEGMENT, 
                    target.ULP_UPDATECOUNT=src.ULP_UPDATECOUNT, 
                    target.ULP_UPDATED=src.ULP_UPDATED, 
                    target.ULP_UPDATEDBY=src.ULP_UPDATEDBY, 
                    target.ULP_USERCODE=src.ULP_USERCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ULP_CODE, 
                    ULP_CREATED, 
                    ULP_CREATEDBY, 
                    ULP_DEFAULT, 
                    ULP_DESC, 
                    ULP_GROUPSEGMENTS, 
                    ULP_HIDEROUES, 
                    ULP_HIDESEGMENTS, 
                    ULP_LASTSAVED, 
                    ULP_LODEFAULTCOLOR, 
                    ULP_LODEFAULTFILTER, 
                    ULP_LOINCLUDERELATED, 
                    ULP_LOLINEARREF, 
                    ULP_LOPOINTSOFINT, 
                    ULP_LORELATEDEQ, 
                    ULP_LORELATEDPART, 
                    ULP_LOROUTEANDSEGMENT, 
                    ULP_UPDATECOUNT, 
                    ULP_UPDATED, 
                    ULP_UPDATEDBY, 
                    ULP_USERCODE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ULP_CODE, 
                    src.ULP_CREATED, 
                    src.ULP_CREATEDBY, 
                    src.ULP_DEFAULT, 
                    src.ULP_DESC, 
                    src.ULP_GROUPSEGMENTS, 
                    src.ULP_HIDEROUES, 
                    src.ULP_HIDESEGMENTS, 
                    src.ULP_LASTSAVED, 
                    src.ULP_LODEFAULTCOLOR, 
                    src.ULP_LODEFAULTFILTER, 
                    src.ULP_LOINCLUDERELATED, 
                    src.ULP_LOLINEARREF, 
                    src.ULP_LOPOINTSOFINT, 
                    src.ULP_LORELATEDEQ, 
                    src.ULP_LORELATEDPART, 
                    src.ULP_LOROUTEANDSEGMENT, 
                    src.ULP_UPDATECOUNT, 
                    src.ULP_UPDATED, 
                    src.ULP_UPDATEDBY, 
                    src.ULP_USERCODE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5USERLINEARPREFERENCES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ULP_CODE, 
            strm.ULP_CREATED, 
            strm.ULP_CREATEDBY, 
            strm.ULP_DEFAULT, 
            strm.ULP_DESC, 
            strm.ULP_GROUPSEGMENTS, 
            strm.ULP_HIDEROUES, 
            strm.ULP_HIDESEGMENTS, 
            strm.ULP_LASTSAVED, 
            strm.ULP_LODEFAULTCOLOR, 
            strm.ULP_LODEFAULTFILTER, 
            strm.ULP_LOINCLUDERELATED, 
            strm.ULP_LOLINEARREF, 
            strm.ULP_LOPOINTSOFINT, 
            strm.ULP_LORELATEDEQ, 
            strm.ULP_LORELATEDPART, 
            strm.ULP_LOROUTEANDSEGMENT, 
            strm.ULP_UPDATECOUNT, 
            strm.ULP_UPDATED, 
            strm.ULP_UPDATEDBY, 
            strm.ULP_USERCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ULP_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERLINEARPREFERENCES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ULP_CODE=src.ULP_CODE) OR (target.ULP_CODE IS NULL AND src.ULP_CODE IS NULL)) 
                when matched then update set
                    target.ULP_CODE=src.ULP_CODE, 
                    target.ULP_CREATED=src.ULP_CREATED, 
                    target.ULP_CREATEDBY=src.ULP_CREATEDBY, 
                    target.ULP_DEFAULT=src.ULP_DEFAULT, 
                    target.ULP_DESC=src.ULP_DESC, 
                    target.ULP_GROUPSEGMENTS=src.ULP_GROUPSEGMENTS, 
                    target.ULP_HIDEROUES=src.ULP_HIDEROUES, 
                    target.ULP_HIDESEGMENTS=src.ULP_HIDESEGMENTS, 
                    target.ULP_LASTSAVED=src.ULP_LASTSAVED, 
                    target.ULP_LODEFAULTCOLOR=src.ULP_LODEFAULTCOLOR, 
                    target.ULP_LODEFAULTFILTER=src.ULP_LODEFAULTFILTER, 
                    target.ULP_LOINCLUDERELATED=src.ULP_LOINCLUDERELATED, 
                    target.ULP_LOLINEARREF=src.ULP_LOLINEARREF, 
                    target.ULP_LOPOINTSOFINT=src.ULP_LOPOINTSOFINT, 
                    target.ULP_LORELATEDEQ=src.ULP_LORELATEDEQ, 
                    target.ULP_LORELATEDPART=src.ULP_LORELATEDPART, 
                    target.ULP_LOROUTEANDSEGMENT=src.ULP_LOROUTEANDSEGMENT, 
                    target.ULP_UPDATECOUNT=src.ULP_UPDATECOUNT, 
                    target.ULP_UPDATED=src.ULP_UPDATED, 
                    target.ULP_UPDATEDBY=src.ULP_UPDATEDBY, 
                    target.ULP_USERCODE=src.ULP_USERCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ULP_CODE, ULP_CREATED, ULP_CREATEDBY, ULP_DEFAULT, ULP_DESC, ULP_GROUPSEGMENTS, ULP_HIDEROUES, ULP_HIDESEGMENTS, ULP_LASTSAVED, ULP_LODEFAULTCOLOR, ULP_LODEFAULTFILTER, ULP_LOINCLUDERELATED, ULP_LOLINEARREF, ULP_LOPOINTSOFINT, ULP_LORELATEDEQ, ULP_LORELATEDPART, ULP_LOROUTEANDSEGMENT, ULP_UPDATECOUNT, ULP_UPDATED, ULP_UPDATEDBY, ULP_USERCODE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ULP_CODE, 
                    src.ULP_CREATED, 
                    src.ULP_CREATEDBY, 
                    src.ULP_DEFAULT, 
                    src.ULP_DESC, 
                    src.ULP_GROUPSEGMENTS, 
                    src.ULP_HIDEROUES, 
                    src.ULP_HIDESEGMENTS, 
                    src.ULP_LASTSAVED, 
                    src.ULP_LODEFAULTCOLOR, 
                    src.ULP_LODEFAULTFILTER, 
                    src.ULP_LOINCLUDERELATED, 
                    src.ULP_LOLINEARREF, 
                    src.ULP_LOPOINTSOFINT, 
                    src.ULP_LORELATEDEQ, 
                    src.ULP_LORELATEDPART, 
                    src.ULP_LOROUTEANDSEGMENT, 
                    src.ULP_UPDATECOUNT, 
                    src.ULP_UPDATED, 
                    src.ULP_UPDATEDBY, 
                    src.ULP_USERCODE, 
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