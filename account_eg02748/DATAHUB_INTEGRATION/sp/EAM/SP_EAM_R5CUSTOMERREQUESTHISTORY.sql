create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CUSTOMERREQUESTHISTORY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CUSTOMERREQUESTHISTORY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CUSTOMERREQUESTHISTORY as target using (SELECT * FROM (SELECT 
            strm.CRH_CALLCENTERCODE, 
            strm.CRH_CALLCENTER_ORG, 
            strm.CRH_COMMENT, 
            strm.CRH_EVENT, 
            strm.CRH_EVENTTYPE, 
            strm.CRH_FIELD, 
            strm.CRH_LASTSAVED, 
            strm.CRH_NEWVALUE, 
            strm.CRH_OLDVALUE, 
            strm.CRH_PK, 
            strm.CRH_RENTITY, 
            strm.CRH_REQDATE, 
            strm.CRH_UPDATECOUNT, 
            strm.CRH_USERCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CRH_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CUSTOMERREQUESTHISTORY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CRH_PK=src.CRH_PK) OR (target.CRH_PK IS NULL AND src.CRH_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CRH_CALLCENTERCODE=src.CRH_CALLCENTERCODE, 
                    target.CRH_CALLCENTER_ORG=src.CRH_CALLCENTER_ORG, 
                    target.CRH_COMMENT=src.CRH_COMMENT, 
                    target.CRH_EVENT=src.CRH_EVENT, 
                    target.CRH_EVENTTYPE=src.CRH_EVENTTYPE, 
                    target.CRH_FIELD=src.CRH_FIELD, 
                    target.CRH_LASTSAVED=src.CRH_LASTSAVED, 
                    target.CRH_NEWVALUE=src.CRH_NEWVALUE, 
                    target.CRH_OLDVALUE=src.CRH_OLDVALUE, 
                    target.CRH_PK=src.CRH_PK, 
                    target.CRH_RENTITY=src.CRH_RENTITY, 
                    target.CRH_REQDATE=src.CRH_REQDATE, 
                    target.CRH_UPDATECOUNT=src.CRH_UPDATECOUNT, 
                    target.CRH_USERCODE=src.CRH_USERCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CRH_CALLCENTERCODE, 
                    CRH_CALLCENTER_ORG, 
                    CRH_COMMENT, 
                    CRH_EVENT, 
                    CRH_EVENTTYPE, 
                    CRH_FIELD, 
                    CRH_LASTSAVED, 
                    CRH_NEWVALUE, 
                    CRH_OLDVALUE, 
                    CRH_PK, 
                    CRH_RENTITY, 
                    CRH_REQDATE, 
                    CRH_UPDATECOUNT, 
                    CRH_USERCODE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CRH_CALLCENTERCODE, 
                    src.CRH_CALLCENTER_ORG, 
                    src.CRH_COMMENT, 
                    src.CRH_EVENT, 
                    src.CRH_EVENTTYPE, 
                    src.CRH_FIELD, 
                    src.CRH_LASTSAVED, 
                    src.CRH_NEWVALUE, 
                    src.CRH_OLDVALUE, 
                    src.CRH_PK, 
                    src.CRH_RENTITY, 
                    src.CRH_REQDATE, 
                    src.CRH_UPDATECOUNT, 
                    src.CRH_USERCODE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CUSTOMERREQUESTHISTORY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CRH_CALLCENTERCODE, 
            strm.CRH_CALLCENTER_ORG, 
            strm.CRH_COMMENT, 
            strm.CRH_EVENT, 
            strm.CRH_EVENTTYPE, 
            strm.CRH_FIELD, 
            strm.CRH_LASTSAVED, 
            strm.CRH_NEWVALUE, 
            strm.CRH_OLDVALUE, 
            strm.CRH_PK, 
            strm.CRH_RENTITY, 
            strm.CRH_REQDATE, 
            strm.CRH_UPDATECOUNT, 
            strm.CRH_USERCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CRH_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CUSTOMERREQUESTHISTORY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CRH_PK=src.CRH_PK) OR (target.CRH_PK IS NULL AND src.CRH_PK IS NULL)) 
                when matched then update set
                    target.CRH_CALLCENTERCODE=src.CRH_CALLCENTERCODE, 
                    target.CRH_CALLCENTER_ORG=src.CRH_CALLCENTER_ORG, 
                    target.CRH_COMMENT=src.CRH_COMMENT, 
                    target.CRH_EVENT=src.CRH_EVENT, 
                    target.CRH_EVENTTYPE=src.CRH_EVENTTYPE, 
                    target.CRH_FIELD=src.CRH_FIELD, 
                    target.CRH_LASTSAVED=src.CRH_LASTSAVED, 
                    target.CRH_NEWVALUE=src.CRH_NEWVALUE, 
                    target.CRH_OLDVALUE=src.CRH_OLDVALUE, 
                    target.CRH_PK=src.CRH_PK, 
                    target.CRH_RENTITY=src.CRH_RENTITY, 
                    target.CRH_REQDATE=src.CRH_REQDATE, 
                    target.CRH_UPDATECOUNT=src.CRH_UPDATECOUNT, 
                    target.CRH_USERCODE=src.CRH_USERCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CRH_CALLCENTERCODE, CRH_CALLCENTER_ORG, CRH_COMMENT, CRH_EVENT, CRH_EVENTTYPE, CRH_FIELD, CRH_LASTSAVED, CRH_NEWVALUE, CRH_OLDVALUE, CRH_PK, CRH_RENTITY, CRH_REQDATE, CRH_UPDATECOUNT, CRH_USERCODE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CRH_CALLCENTERCODE, 
                    src.CRH_CALLCENTER_ORG, 
                    src.CRH_COMMENT, 
                    src.CRH_EVENT, 
                    src.CRH_EVENTTYPE, 
                    src.CRH_FIELD, 
                    src.CRH_LASTSAVED, 
                    src.CRH_NEWVALUE, 
                    src.CRH_OLDVALUE, 
                    src.CRH_PK, 
                    src.CRH_RENTITY, 
                    src.CRH_REQDATE, 
                    src.CRH_UPDATECOUNT, 
                    src.CRH_USERCODE, 
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