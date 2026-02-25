create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DUXLAYOUT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DUXLAYOUT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DUXLAYOUT as target using (SELECT * FROM (SELECT 
            strm.DXL_ATTRIBUTE, 
            strm.DXL_DEFAULTVALUE, 
            strm.DXL_ELEMENTID, 
            strm.DXL_ELEMENTTYPE, 
            strm.DXL_FIELDINFO, 
            strm.DXL_FIELDSIZE, 
            strm.DXL_LASTSAVED, 
            strm.DXL_NEWCARD, 
            strm.DXL_PAGENAME, 
            strm.DXL_POSITIONINGROUP, 
            strm.DXL_PRESENTINJSP, 
            strm.DXL_RADIOOPTIONS, 
            strm.DXL_SECTION, 
            strm.DXL_SECTIONLABEL, 
            strm.DXL_SECTIONPOSITION, 
            strm.DXL_SYSTEMATTRIBUTE, 
            strm.DXL_UPDATECOUNT, 
            strm.DXL_USERGROUP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DXL_USERGROUP,
            strm.DXL_PAGENAME,
            strm.DXL_ELEMENTID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DUXLAYOUT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DXL_USERGROUP=src.DXL_USERGROUP) OR (target.DXL_USERGROUP IS NULL AND src.DXL_USERGROUP IS NULL)) AND
            ((target.DXL_PAGENAME=src.DXL_PAGENAME) OR (target.DXL_PAGENAME IS NULL AND src.DXL_PAGENAME IS NULL)) AND
            ((target.DXL_ELEMENTID=src.DXL_ELEMENTID) OR (target.DXL_ELEMENTID IS NULL AND src.DXL_ELEMENTID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DXL_ATTRIBUTE=src.DXL_ATTRIBUTE, 
                    target.DXL_DEFAULTVALUE=src.DXL_DEFAULTVALUE, 
                    target.DXL_ELEMENTID=src.DXL_ELEMENTID, 
                    target.DXL_ELEMENTTYPE=src.DXL_ELEMENTTYPE, 
                    target.DXL_FIELDINFO=src.DXL_FIELDINFO, 
                    target.DXL_FIELDSIZE=src.DXL_FIELDSIZE, 
                    target.DXL_LASTSAVED=src.DXL_LASTSAVED, 
                    target.DXL_NEWCARD=src.DXL_NEWCARD, 
                    target.DXL_PAGENAME=src.DXL_PAGENAME, 
                    target.DXL_POSITIONINGROUP=src.DXL_POSITIONINGROUP, 
                    target.DXL_PRESENTINJSP=src.DXL_PRESENTINJSP, 
                    target.DXL_RADIOOPTIONS=src.DXL_RADIOOPTIONS, 
                    target.DXL_SECTION=src.DXL_SECTION, 
                    target.DXL_SECTIONLABEL=src.DXL_SECTIONLABEL, 
                    target.DXL_SECTIONPOSITION=src.DXL_SECTIONPOSITION, 
                    target.DXL_SYSTEMATTRIBUTE=src.DXL_SYSTEMATTRIBUTE, 
                    target.DXL_UPDATECOUNT=src.DXL_UPDATECOUNT, 
                    target.DXL_USERGROUP=src.DXL_USERGROUP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DXL_ATTRIBUTE, 
                    DXL_DEFAULTVALUE, 
                    DXL_ELEMENTID, 
                    DXL_ELEMENTTYPE, 
                    DXL_FIELDINFO, 
                    DXL_FIELDSIZE, 
                    DXL_LASTSAVED, 
                    DXL_NEWCARD, 
                    DXL_PAGENAME, 
                    DXL_POSITIONINGROUP, 
                    DXL_PRESENTINJSP, 
                    DXL_RADIOOPTIONS, 
                    DXL_SECTION, 
                    DXL_SECTIONLABEL, 
                    DXL_SECTIONPOSITION, 
                    DXL_SYSTEMATTRIBUTE, 
                    DXL_UPDATECOUNT, 
                    DXL_USERGROUP, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DXL_ATTRIBUTE, 
                    src.DXL_DEFAULTVALUE, 
                    src.DXL_ELEMENTID, 
                    src.DXL_ELEMENTTYPE, 
                    src.DXL_FIELDINFO, 
                    src.DXL_FIELDSIZE, 
                    src.DXL_LASTSAVED, 
                    src.DXL_NEWCARD, 
                    src.DXL_PAGENAME, 
                    src.DXL_POSITIONINGROUP, 
                    src.DXL_PRESENTINJSP, 
                    src.DXL_RADIOOPTIONS, 
                    src.DXL_SECTION, 
                    src.DXL_SECTIONLABEL, 
                    src.DXL_SECTIONPOSITION, 
                    src.DXL_SYSTEMATTRIBUTE, 
                    src.DXL_UPDATECOUNT, 
                    src.DXL_USERGROUP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DUXLAYOUT_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DXL_ATTRIBUTE, 
            strm.DXL_DEFAULTVALUE, 
            strm.DXL_ELEMENTID, 
            strm.DXL_ELEMENTTYPE, 
            strm.DXL_FIELDINFO, 
            strm.DXL_FIELDSIZE, 
            strm.DXL_LASTSAVED, 
            strm.DXL_NEWCARD, 
            strm.DXL_PAGENAME, 
            strm.DXL_POSITIONINGROUP, 
            strm.DXL_PRESENTINJSP, 
            strm.DXL_RADIOOPTIONS, 
            strm.DXL_SECTION, 
            strm.DXL_SECTIONLABEL, 
            strm.DXL_SECTIONPOSITION, 
            strm.DXL_SYSTEMATTRIBUTE, 
            strm.DXL_UPDATECOUNT, 
            strm.DXL_USERGROUP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DXL_USERGROUP,
            strm.DXL_PAGENAME,
            strm.DXL_ELEMENTID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DUXLAYOUT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DXL_USERGROUP=src.DXL_USERGROUP) OR (target.DXL_USERGROUP IS NULL AND src.DXL_USERGROUP IS NULL)) AND
            ((target.DXL_PAGENAME=src.DXL_PAGENAME) OR (target.DXL_PAGENAME IS NULL AND src.DXL_PAGENAME IS NULL)) AND
            ((target.DXL_ELEMENTID=src.DXL_ELEMENTID) OR (target.DXL_ELEMENTID IS NULL AND src.DXL_ELEMENTID IS NULL)) 
                when matched then update set
                    target.DXL_ATTRIBUTE=src.DXL_ATTRIBUTE, 
                    target.DXL_DEFAULTVALUE=src.DXL_DEFAULTVALUE, 
                    target.DXL_ELEMENTID=src.DXL_ELEMENTID, 
                    target.DXL_ELEMENTTYPE=src.DXL_ELEMENTTYPE, 
                    target.DXL_FIELDINFO=src.DXL_FIELDINFO, 
                    target.DXL_FIELDSIZE=src.DXL_FIELDSIZE, 
                    target.DXL_LASTSAVED=src.DXL_LASTSAVED, 
                    target.DXL_NEWCARD=src.DXL_NEWCARD, 
                    target.DXL_PAGENAME=src.DXL_PAGENAME, 
                    target.DXL_POSITIONINGROUP=src.DXL_POSITIONINGROUP, 
                    target.DXL_PRESENTINJSP=src.DXL_PRESENTINJSP, 
                    target.DXL_RADIOOPTIONS=src.DXL_RADIOOPTIONS, 
                    target.DXL_SECTION=src.DXL_SECTION, 
                    target.DXL_SECTIONLABEL=src.DXL_SECTIONLABEL, 
                    target.DXL_SECTIONPOSITION=src.DXL_SECTIONPOSITION, 
                    target.DXL_SYSTEMATTRIBUTE=src.DXL_SYSTEMATTRIBUTE, 
                    target.DXL_UPDATECOUNT=src.DXL_UPDATECOUNT, 
                    target.DXL_USERGROUP=src.DXL_USERGROUP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DXL_ATTRIBUTE, DXL_DEFAULTVALUE, DXL_ELEMENTID, DXL_ELEMENTTYPE, DXL_FIELDINFO, DXL_FIELDSIZE, DXL_LASTSAVED, DXL_NEWCARD, DXL_PAGENAME, DXL_POSITIONINGROUP, DXL_PRESENTINJSP, DXL_RADIOOPTIONS, DXL_SECTION, DXL_SECTIONLABEL, DXL_SECTIONPOSITION, DXL_SYSTEMATTRIBUTE, DXL_UPDATECOUNT, DXL_USERGROUP, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DXL_ATTRIBUTE, 
                    src.DXL_DEFAULTVALUE, 
                    src.DXL_ELEMENTID, 
                    src.DXL_ELEMENTTYPE, 
                    src.DXL_FIELDINFO, 
                    src.DXL_FIELDSIZE, 
                    src.DXL_LASTSAVED, 
                    src.DXL_NEWCARD, 
                    src.DXL_PAGENAME, 
                    src.DXL_POSITIONINGROUP, 
                    src.DXL_PRESENTINJSP, 
                    src.DXL_RADIOOPTIONS, 
                    src.DXL_SECTION, 
                    src.DXL_SECTIONLABEL, 
                    src.DXL_SECTIONPOSITION, 
                    src.DXL_SYSTEMATTRIBUTE, 
                    src.DXL_UPDATECOUNT, 
                    src.DXL_USERGROUP, 
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