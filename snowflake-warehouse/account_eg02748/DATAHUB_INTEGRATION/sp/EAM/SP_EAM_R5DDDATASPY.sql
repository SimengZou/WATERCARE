create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DDDATASPY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DDDATASPY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DDDATASPY as target using (SELECT * FROM (SELECT 
            strm.DDS_AUTORUN, 
            strm.DDS_BLACKLISTVIOLATIONS, 
            strm.DDS_BOTNAME, 
            strm.DDS_CLIENTROWS, 
            strm.DDS_CREATEDSTAMP, 
            strm.DDS_DDSPYFILTER, 
            strm.DDS_DDSPYID, 
            strm.DDS_DDSPYNAME, 
            strm.DDS_DEFAULTFLAG, 
            strm.DDS_DISPLAYROW, 
            strm.DDS_FIELDLIST, 
            strm.DDS_FIELDLIST_PORTLET, 
            strm.DDS_FILTERSTRXML, 
            strm.DDS_GLOBALDATASPY, 
            strm.DDS_GRIDID, 
            strm.DDS_HINTS, 
            strm.DDS_LASTSAVED, 
            strm.DDS_MEKEY, 
            strm.DDS_ORGANIZATION, 
            strm.DDS_OWNER, 
            strm.DDS_PORTLETROWS, 
            strm.DDS_SCOPE, 
            strm.DDS_SECURITYDATASPY, 
            strm.DDS_SORTSTRXML, 
            strm.DDS_TYPE, 
            strm.DDS_UPDATEABLE, 
            strm.DDS_UPDATEBYUSER, 
            strm.DDS_UPDATECOUNT, 
            strm.DDS_UPDATED, 
            strm.DDS_UPDATESTAMP, 
            strm.DDS_USERFILTER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DDS_DDSPYID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DDDATASPY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DDS_DDSPYID=src.DDS_DDSPYID) OR (target.DDS_DDSPYID IS NULL AND src.DDS_DDSPYID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DDS_AUTORUN=src.DDS_AUTORUN, 
                    target.DDS_BLACKLISTVIOLATIONS=src.DDS_BLACKLISTVIOLATIONS, 
                    target.DDS_BOTNAME=src.DDS_BOTNAME, 
                    target.DDS_CLIENTROWS=src.DDS_CLIENTROWS, 
                    target.DDS_CREATEDSTAMP=src.DDS_CREATEDSTAMP, 
                    target.DDS_DDSPYFILTER=src.DDS_DDSPYFILTER, 
                    target.DDS_DDSPYID=src.DDS_DDSPYID, 
                    target.DDS_DDSPYNAME=src.DDS_DDSPYNAME, 
                    target.DDS_DEFAULTFLAG=src.DDS_DEFAULTFLAG, 
                    target.DDS_DISPLAYROW=src.DDS_DISPLAYROW, 
                    target.DDS_FIELDLIST=src.DDS_FIELDLIST, 
                    target.DDS_FIELDLIST_PORTLET=src.DDS_FIELDLIST_PORTLET, 
                    target.DDS_FILTERSTRXML=src.DDS_FILTERSTRXML, 
                    target.DDS_GLOBALDATASPY=src.DDS_GLOBALDATASPY, 
                    target.DDS_GRIDID=src.DDS_GRIDID, 
                    target.DDS_HINTS=src.DDS_HINTS, 
                    target.DDS_LASTSAVED=src.DDS_LASTSAVED, 
                    target.DDS_MEKEY=src.DDS_MEKEY, 
                    target.DDS_ORGANIZATION=src.DDS_ORGANIZATION, 
                    target.DDS_OWNER=src.DDS_OWNER, 
                    target.DDS_PORTLETROWS=src.DDS_PORTLETROWS, 
                    target.DDS_SCOPE=src.DDS_SCOPE, 
                    target.DDS_SECURITYDATASPY=src.DDS_SECURITYDATASPY, 
                    target.DDS_SORTSTRXML=src.DDS_SORTSTRXML, 
                    target.DDS_TYPE=src.DDS_TYPE, 
                    target.DDS_UPDATEABLE=src.DDS_UPDATEABLE, 
                    target.DDS_UPDATEBYUSER=src.DDS_UPDATEBYUSER, 
                    target.DDS_UPDATECOUNT=src.DDS_UPDATECOUNT, 
                    target.DDS_UPDATED=src.DDS_UPDATED, 
                    target.DDS_UPDATESTAMP=src.DDS_UPDATESTAMP, 
                    target.DDS_USERFILTER=src.DDS_USERFILTER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DDS_AUTORUN, 
                    DDS_BLACKLISTVIOLATIONS, 
                    DDS_BOTNAME, 
                    DDS_CLIENTROWS, 
                    DDS_CREATEDSTAMP, 
                    DDS_DDSPYFILTER, 
                    DDS_DDSPYID, 
                    DDS_DDSPYNAME, 
                    DDS_DEFAULTFLAG, 
                    DDS_DISPLAYROW, 
                    DDS_FIELDLIST, 
                    DDS_FIELDLIST_PORTLET, 
                    DDS_FILTERSTRXML, 
                    DDS_GLOBALDATASPY, 
                    DDS_GRIDID, 
                    DDS_HINTS, 
                    DDS_LASTSAVED, 
                    DDS_MEKEY, 
                    DDS_ORGANIZATION, 
                    DDS_OWNER, 
                    DDS_PORTLETROWS, 
                    DDS_SCOPE, 
                    DDS_SECURITYDATASPY, 
                    DDS_SORTSTRXML, 
                    DDS_TYPE, 
                    DDS_UPDATEABLE, 
                    DDS_UPDATEBYUSER, 
                    DDS_UPDATECOUNT, 
                    DDS_UPDATED, 
                    DDS_UPDATESTAMP, 
                    DDS_USERFILTER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DDS_AUTORUN, 
                    src.DDS_BLACKLISTVIOLATIONS, 
                    src.DDS_BOTNAME, 
                    src.DDS_CLIENTROWS, 
                    src.DDS_CREATEDSTAMP, 
                    src.DDS_DDSPYFILTER, 
                    src.DDS_DDSPYID, 
                    src.DDS_DDSPYNAME, 
                    src.DDS_DEFAULTFLAG, 
                    src.DDS_DISPLAYROW, 
                    src.DDS_FIELDLIST, 
                    src.DDS_FIELDLIST_PORTLET, 
                    src.DDS_FILTERSTRXML, 
                    src.DDS_GLOBALDATASPY, 
                    src.DDS_GRIDID, 
                    src.DDS_HINTS, 
                    src.DDS_LASTSAVED, 
                    src.DDS_MEKEY, 
                    src.DDS_ORGANIZATION, 
                    src.DDS_OWNER, 
                    src.DDS_PORTLETROWS, 
                    src.DDS_SCOPE, 
                    src.DDS_SECURITYDATASPY, 
                    src.DDS_SORTSTRXML, 
                    src.DDS_TYPE, 
                    src.DDS_UPDATEABLE, 
                    src.DDS_UPDATEBYUSER, 
                    src.DDS_UPDATECOUNT, 
                    src.DDS_UPDATED, 
                    src.DDS_UPDATESTAMP, 
                    src.DDS_USERFILTER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DDDATASPY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DDS_AUTORUN, 
            strm.DDS_BLACKLISTVIOLATIONS, 
            strm.DDS_BOTNAME, 
            strm.DDS_CLIENTROWS, 
            strm.DDS_CREATEDSTAMP, 
            strm.DDS_DDSPYFILTER, 
            strm.DDS_DDSPYID, 
            strm.DDS_DDSPYNAME, 
            strm.DDS_DEFAULTFLAG, 
            strm.DDS_DISPLAYROW, 
            strm.DDS_FIELDLIST, 
            strm.DDS_FIELDLIST_PORTLET, 
            strm.DDS_FILTERSTRXML, 
            strm.DDS_GLOBALDATASPY, 
            strm.DDS_GRIDID, 
            strm.DDS_HINTS, 
            strm.DDS_LASTSAVED, 
            strm.DDS_MEKEY, 
            strm.DDS_ORGANIZATION, 
            strm.DDS_OWNER, 
            strm.DDS_PORTLETROWS, 
            strm.DDS_SCOPE, 
            strm.DDS_SECURITYDATASPY, 
            strm.DDS_SORTSTRXML, 
            strm.DDS_TYPE, 
            strm.DDS_UPDATEABLE, 
            strm.DDS_UPDATEBYUSER, 
            strm.DDS_UPDATECOUNT, 
            strm.DDS_UPDATED, 
            strm.DDS_UPDATESTAMP, 
            strm.DDS_USERFILTER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DDS_DDSPYID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DDDATASPY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DDS_DDSPYID=src.DDS_DDSPYID) OR (target.DDS_DDSPYID IS NULL AND src.DDS_DDSPYID IS NULL)) 
                when matched then update set
                    target.DDS_AUTORUN=src.DDS_AUTORUN, 
                    target.DDS_BLACKLISTVIOLATIONS=src.DDS_BLACKLISTVIOLATIONS, 
                    target.DDS_BOTNAME=src.DDS_BOTNAME, 
                    target.DDS_CLIENTROWS=src.DDS_CLIENTROWS, 
                    target.DDS_CREATEDSTAMP=src.DDS_CREATEDSTAMP, 
                    target.DDS_DDSPYFILTER=src.DDS_DDSPYFILTER, 
                    target.DDS_DDSPYID=src.DDS_DDSPYID, 
                    target.DDS_DDSPYNAME=src.DDS_DDSPYNAME, 
                    target.DDS_DEFAULTFLAG=src.DDS_DEFAULTFLAG, 
                    target.DDS_DISPLAYROW=src.DDS_DISPLAYROW, 
                    target.DDS_FIELDLIST=src.DDS_FIELDLIST, 
                    target.DDS_FIELDLIST_PORTLET=src.DDS_FIELDLIST_PORTLET, 
                    target.DDS_FILTERSTRXML=src.DDS_FILTERSTRXML, 
                    target.DDS_GLOBALDATASPY=src.DDS_GLOBALDATASPY, 
                    target.DDS_GRIDID=src.DDS_GRIDID, 
                    target.DDS_HINTS=src.DDS_HINTS, 
                    target.DDS_LASTSAVED=src.DDS_LASTSAVED, 
                    target.DDS_MEKEY=src.DDS_MEKEY, 
                    target.DDS_ORGANIZATION=src.DDS_ORGANIZATION, 
                    target.DDS_OWNER=src.DDS_OWNER, 
                    target.DDS_PORTLETROWS=src.DDS_PORTLETROWS, 
                    target.DDS_SCOPE=src.DDS_SCOPE, 
                    target.DDS_SECURITYDATASPY=src.DDS_SECURITYDATASPY, 
                    target.DDS_SORTSTRXML=src.DDS_SORTSTRXML, 
                    target.DDS_TYPE=src.DDS_TYPE, 
                    target.DDS_UPDATEABLE=src.DDS_UPDATEABLE, 
                    target.DDS_UPDATEBYUSER=src.DDS_UPDATEBYUSER, 
                    target.DDS_UPDATECOUNT=src.DDS_UPDATECOUNT, 
                    target.DDS_UPDATED=src.DDS_UPDATED, 
                    target.DDS_UPDATESTAMP=src.DDS_UPDATESTAMP, 
                    target.DDS_USERFILTER=src.DDS_USERFILTER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DDS_AUTORUN, DDS_BLACKLISTVIOLATIONS, DDS_BOTNAME, DDS_CLIENTROWS, DDS_CREATEDSTAMP, DDS_DDSPYFILTER, DDS_DDSPYID, DDS_DDSPYNAME, DDS_DEFAULTFLAG, DDS_DISPLAYROW, DDS_FIELDLIST, DDS_FIELDLIST_PORTLET, DDS_FILTERSTRXML, DDS_GLOBALDATASPY, DDS_GRIDID, DDS_HINTS, DDS_LASTSAVED, DDS_MEKEY, DDS_ORGANIZATION, DDS_OWNER, DDS_PORTLETROWS, DDS_SCOPE, DDS_SECURITYDATASPY, DDS_SORTSTRXML, DDS_TYPE, DDS_UPDATEABLE, DDS_UPDATEBYUSER, DDS_UPDATECOUNT, DDS_UPDATED, DDS_UPDATESTAMP, DDS_USERFILTER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DDS_AUTORUN, 
                    src.DDS_BLACKLISTVIOLATIONS, 
                    src.DDS_BOTNAME, 
                    src.DDS_CLIENTROWS, 
                    src.DDS_CREATEDSTAMP, 
                    src.DDS_DDSPYFILTER, 
                    src.DDS_DDSPYID, 
                    src.DDS_DDSPYNAME, 
                    src.DDS_DEFAULTFLAG, 
                    src.DDS_DISPLAYROW, 
                    src.DDS_FIELDLIST, 
                    src.DDS_FIELDLIST_PORTLET, 
                    src.DDS_FILTERSTRXML, 
                    src.DDS_GLOBALDATASPY, 
                    src.DDS_GRIDID, 
                    src.DDS_HINTS, 
                    src.DDS_LASTSAVED, 
                    src.DDS_MEKEY, 
                    src.DDS_ORGANIZATION, 
                    src.DDS_OWNER, 
                    src.DDS_PORTLETROWS, 
                    src.DDS_SCOPE, 
                    src.DDS_SECURITYDATASPY, 
                    src.DDS_SORTSTRXML, 
                    src.DDS_TYPE, 
                    src.DDS_UPDATEABLE, 
                    src.DDS_UPDATEBYUSER, 
                    src.DDS_UPDATECOUNT, 
                    src.DDS_UPDATED, 
                    src.DDS_UPDATESTAMP, 
                    src.DDS_USERFILTER, 
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