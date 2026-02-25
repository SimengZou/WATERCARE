create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5GRID()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5GRID_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5GRID as target using (SELECT * FROM (SELECT 
            strm.GRD_ACTIVE, 
            strm.GRD_BASEQUERY, 
            strm.GRD_BASEQUERY_MULTIORG, 
            strm.GRD_BOTFUNCTION, 
            strm.GRD_COMMITFLAG, 
            strm.GRD_COMPLEX, 
            strm.GRD_CUSTOMFIELDCODE, 
            strm.GRD_DESC, 
            strm.GRD_DISPLAYABLELIST, 
            strm.GRD_DISPLAYABLE_MULTIORG, 
            strm.GRD_DISTINCT, 
            strm.GRD_FILTERABLELIST, 
            strm.GRD_FILTERABLE_MULTIORG, 
            strm.GRD_GISDATAFILTER, 
            strm.GRD_GISWOFIELDMAP, 
            strm.GRD_GRIDID, 
            strm.GRD_GRIDNAME, 
            strm.GRD_GRIDTYPE, 
            strm.GRD_HINTS, 
            strm.GRD_KEYFIELDS, 
            strm.GRD_KEYFIELDS_MULTIORG, 
            strm.GRD_LASTSAVED, 
            strm.GRD_MAXGRIDCOST, 
            strm.GRD_MOBILE, 
            strm.GRD_NOSCREENDESIGNER, 
            strm.GRD_OPTIMIZERON, 
            strm.GRD_ORGCOLNAME, 
            strm.GRD_PORTLETFLAG, 
            strm.GRD_SECENTITY, 
            strm.GRD_SORTABLELIST, 
            strm.GRD_SORTABLE_MULTIORG, 
            strm.GRD_TAB, 
            strm.GRD_UNITS, 
            strm.GRD_UPDATECOUNT, 
            strm.GRD_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.GRD_GRIDID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRID as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.GRD_GRIDID=src.GRD_GRIDID) OR (target.GRD_GRIDID IS NULL AND src.GRD_GRIDID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.GRD_ACTIVE=src.GRD_ACTIVE, 
                    target.GRD_BASEQUERY=src.GRD_BASEQUERY, 
                    target.GRD_BASEQUERY_MULTIORG=src.GRD_BASEQUERY_MULTIORG, 
                    target.GRD_BOTFUNCTION=src.GRD_BOTFUNCTION, 
                    target.GRD_COMMITFLAG=src.GRD_COMMITFLAG, 
                    target.GRD_COMPLEX=src.GRD_COMPLEX, 
                    target.GRD_CUSTOMFIELDCODE=src.GRD_CUSTOMFIELDCODE, 
                    target.GRD_DESC=src.GRD_DESC, 
                    target.GRD_DISPLAYABLELIST=src.GRD_DISPLAYABLELIST, 
                    target.GRD_DISPLAYABLE_MULTIORG=src.GRD_DISPLAYABLE_MULTIORG, 
                    target.GRD_DISTINCT=src.GRD_DISTINCT, 
                    target.GRD_FILTERABLELIST=src.GRD_FILTERABLELIST, 
                    target.GRD_FILTERABLE_MULTIORG=src.GRD_FILTERABLE_MULTIORG, 
                    target.GRD_GISDATAFILTER=src.GRD_GISDATAFILTER, 
                    target.GRD_GISWOFIELDMAP=src.GRD_GISWOFIELDMAP, 
                    target.GRD_GRIDID=src.GRD_GRIDID, 
                    target.GRD_GRIDNAME=src.GRD_GRIDNAME, 
                    target.GRD_GRIDTYPE=src.GRD_GRIDTYPE, 
                    target.GRD_HINTS=src.GRD_HINTS, 
                    target.GRD_KEYFIELDS=src.GRD_KEYFIELDS, 
                    target.GRD_KEYFIELDS_MULTIORG=src.GRD_KEYFIELDS_MULTIORG, 
                    target.GRD_LASTSAVED=src.GRD_LASTSAVED, 
                    target.GRD_MAXGRIDCOST=src.GRD_MAXGRIDCOST, 
                    target.GRD_MOBILE=src.GRD_MOBILE, 
                    target.GRD_NOSCREENDESIGNER=src.GRD_NOSCREENDESIGNER, 
                    target.GRD_OPTIMIZERON=src.GRD_OPTIMIZERON, 
                    target.GRD_ORGCOLNAME=src.GRD_ORGCOLNAME, 
                    target.GRD_PORTLETFLAG=src.GRD_PORTLETFLAG, 
                    target.GRD_SECENTITY=src.GRD_SECENTITY, 
                    target.GRD_SORTABLELIST=src.GRD_SORTABLELIST, 
                    target.GRD_SORTABLE_MULTIORG=src.GRD_SORTABLE_MULTIORG, 
                    target.GRD_TAB=src.GRD_TAB, 
                    target.GRD_UNITS=src.GRD_UNITS, 
                    target.GRD_UPDATECOUNT=src.GRD_UPDATECOUNT, 
                    target.GRD_UPDATED=src.GRD_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    GRD_ACTIVE, 
                    GRD_BASEQUERY, 
                    GRD_BASEQUERY_MULTIORG, 
                    GRD_BOTFUNCTION, 
                    GRD_COMMITFLAG, 
                    GRD_COMPLEX, 
                    GRD_CUSTOMFIELDCODE, 
                    GRD_DESC, 
                    GRD_DISPLAYABLELIST, 
                    GRD_DISPLAYABLE_MULTIORG, 
                    GRD_DISTINCT, 
                    GRD_FILTERABLELIST, 
                    GRD_FILTERABLE_MULTIORG, 
                    GRD_GISDATAFILTER, 
                    GRD_GISWOFIELDMAP, 
                    GRD_GRIDID, 
                    GRD_GRIDNAME, 
                    GRD_GRIDTYPE, 
                    GRD_HINTS, 
                    GRD_KEYFIELDS, 
                    GRD_KEYFIELDS_MULTIORG, 
                    GRD_LASTSAVED, 
                    GRD_MAXGRIDCOST, 
                    GRD_MOBILE, 
                    GRD_NOSCREENDESIGNER, 
                    GRD_OPTIMIZERON, 
                    GRD_ORGCOLNAME, 
                    GRD_PORTLETFLAG, 
                    GRD_SECENTITY, 
                    GRD_SORTABLELIST, 
                    GRD_SORTABLE_MULTIORG, 
                    GRD_TAB, 
                    GRD_UNITS, 
                    GRD_UPDATECOUNT, 
                    GRD_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.GRD_ACTIVE, 
                    src.GRD_BASEQUERY, 
                    src.GRD_BASEQUERY_MULTIORG, 
                    src.GRD_BOTFUNCTION, 
                    src.GRD_COMMITFLAG, 
                    src.GRD_COMPLEX, 
                    src.GRD_CUSTOMFIELDCODE, 
                    src.GRD_DESC, 
                    src.GRD_DISPLAYABLELIST, 
                    src.GRD_DISPLAYABLE_MULTIORG, 
                    src.GRD_DISTINCT, 
                    src.GRD_FILTERABLELIST, 
                    src.GRD_FILTERABLE_MULTIORG, 
                    src.GRD_GISDATAFILTER, 
                    src.GRD_GISWOFIELDMAP, 
                    src.GRD_GRIDID, 
                    src.GRD_GRIDNAME, 
                    src.GRD_GRIDTYPE, 
                    src.GRD_HINTS, 
                    src.GRD_KEYFIELDS, 
                    src.GRD_KEYFIELDS_MULTIORG, 
                    src.GRD_LASTSAVED, 
                    src.GRD_MAXGRIDCOST, 
                    src.GRD_MOBILE, 
                    src.GRD_NOSCREENDESIGNER, 
                    src.GRD_OPTIMIZERON, 
                    src.GRD_ORGCOLNAME, 
                    src.GRD_PORTLETFLAG, 
                    src.GRD_SECENTITY, 
                    src.GRD_SORTABLELIST, 
                    src.GRD_SORTABLE_MULTIORG, 
                    src.GRD_TAB, 
                    src.GRD_UNITS, 
                    src.GRD_UPDATECOUNT, 
                    src.GRD_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5GRID_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.GRD_ACTIVE, 
            strm.GRD_BASEQUERY, 
            strm.GRD_BASEQUERY_MULTIORG, 
            strm.GRD_BOTFUNCTION, 
            strm.GRD_COMMITFLAG, 
            strm.GRD_COMPLEX, 
            strm.GRD_CUSTOMFIELDCODE, 
            strm.GRD_DESC, 
            strm.GRD_DISPLAYABLELIST, 
            strm.GRD_DISPLAYABLE_MULTIORG, 
            strm.GRD_DISTINCT, 
            strm.GRD_FILTERABLELIST, 
            strm.GRD_FILTERABLE_MULTIORG, 
            strm.GRD_GISDATAFILTER, 
            strm.GRD_GISWOFIELDMAP, 
            strm.GRD_GRIDID, 
            strm.GRD_GRIDNAME, 
            strm.GRD_GRIDTYPE, 
            strm.GRD_HINTS, 
            strm.GRD_KEYFIELDS, 
            strm.GRD_KEYFIELDS_MULTIORG, 
            strm.GRD_LASTSAVED, 
            strm.GRD_MAXGRIDCOST, 
            strm.GRD_MOBILE, 
            strm.GRD_NOSCREENDESIGNER, 
            strm.GRD_OPTIMIZERON, 
            strm.GRD_ORGCOLNAME, 
            strm.GRD_PORTLETFLAG, 
            strm.GRD_SECENTITY, 
            strm.GRD_SORTABLELIST, 
            strm.GRD_SORTABLE_MULTIORG, 
            strm.GRD_TAB, 
            strm.GRD_UNITS, 
            strm.GRD_UPDATECOUNT, 
            strm.GRD_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.GRD_GRIDID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRID as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.GRD_GRIDID=src.GRD_GRIDID) OR (target.GRD_GRIDID IS NULL AND src.GRD_GRIDID IS NULL)) 
                when matched then update set
                    target.GRD_ACTIVE=src.GRD_ACTIVE, 
                    target.GRD_BASEQUERY=src.GRD_BASEQUERY, 
                    target.GRD_BASEQUERY_MULTIORG=src.GRD_BASEQUERY_MULTIORG, 
                    target.GRD_BOTFUNCTION=src.GRD_BOTFUNCTION, 
                    target.GRD_COMMITFLAG=src.GRD_COMMITFLAG, 
                    target.GRD_COMPLEX=src.GRD_COMPLEX, 
                    target.GRD_CUSTOMFIELDCODE=src.GRD_CUSTOMFIELDCODE, 
                    target.GRD_DESC=src.GRD_DESC, 
                    target.GRD_DISPLAYABLELIST=src.GRD_DISPLAYABLELIST, 
                    target.GRD_DISPLAYABLE_MULTIORG=src.GRD_DISPLAYABLE_MULTIORG, 
                    target.GRD_DISTINCT=src.GRD_DISTINCT, 
                    target.GRD_FILTERABLELIST=src.GRD_FILTERABLELIST, 
                    target.GRD_FILTERABLE_MULTIORG=src.GRD_FILTERABLE_MULTIORG, 
                    target.GRD_GISDATAFILTER=src.GRD_GISDATAFILTER, 
                    target.GRD_GISWOFIELDMAP=src.GRD_GISWOFIELDMAP, 
                    target.GRD_GRIDID=src.GRD_GRIDID, 
                    target.GRD_GRIDNAME=src.GRD_GRIDNAME, 
                    target.GRD_GRIDTYPE=src.GRD_GRIDTYPE, 
                    target.GRD_HINTS=src.GRD_HINTS, 
                    target.GRD_KEYFIELDS=src.GRD_KEYFIELDS, 
                    target.GRD_KEYFIELDS_MULTIORG=src.GRD_KEYFIELDS_MULTIORG, 
                    target.GRD_LASTSAVED=src.GRD_LASTSAVED, 
                    target.GRD_MAXGRIDCOST=src.GRD_MAXGRIDCOST, 
                    target.GRD_MOBILE=src.GRD_MOBILE, 
                    target.GRD_NOSCREENDESIGNER=src.GRD_NOSCREENDESIGNER, 
                    target.GRD_OPTIMIZERON=src.GRD_OPTIMIZERON, 
                    target.GRD_ORGCOLNAME=src.GRD_ORGCOLNAME, 
                    target.GRD_PORTLETFLAG=src.GRD_PORTLETFLAG, 
                    target.GRD_SECENTITY=src.GRD_SECENTITY, 
                    target.GRD_SORTABLELIST=src.GRD_SORTABLELIST, 
                    target.GRD_SORTABLE_MULTIORG=src.GRD_SORTABLE_MULTIORG, 
                    target.GRD_TAB=src.GRD_TAB, 
                    target.GRD_UNITS=src.GRD_UNITS, 
                    target.GRD_UPDATECOUNT=src.GRD_UPDATECOUNT, 
                    target.GRD_UPDATED=src.GRD_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( GRD_ACTIVE, GRD_BASEQUERY, GRD_BASEQUERY_MULTIORG, GRD_BOTFUNCTION, GRD_COMMITFLAG, GRD_COMPLEX, GRD_CUSTOMFIELDCODE, GRD_DESC, GRD_DISPLAYABLELIST, GRD_DISPLAYABLE_MULTIORG, GRD_DISTINCT, GRD_FILTERABLELIST, GRD_FILTERABLE_MULTIORG, GRD_GISDATAFILTER, GRD_GISWOFIELDMAP, GRD_GRIDID, GRD_GRIDNAME, GRD_GRIDTYPE, GRD_HINTS, GRD_KEYFIELDS, GRD_KEYFIELDS_MULTIORG, GRD_LASTSAVED, GRD_MAXGRIDCOST, GRD_MOBILE, GRD_NOSCREENDESIGNER, GRD_OPTIMIZERON, GRD_ORGCOLNAME, GRD_PORTLETFLAG, GRD_SECENTITY, GRD_SORTABLELIST, GRD_SORTABLE_MULTIORG, GRD_TAB, GRD_UNITS, GRD_UPDATECOUNT, GRD_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.GRD_ACTIVE, 
                    src.GRD_BASEQUERY, 
                    src.GRD_BASEQUERY_MULTIORG, 
                    src.GRD_BOTFUNCTION, 
                    src.GRD_COMMITFLAG, 
                    src.GRD_COMPLEX, 
                    src.GRD_CUSTOMFIELDCODE, 
                    src.GRD_DESC, 
                    src.GRD_DISPLAYABLELIST, 
                    src.GRD_DISPLAYABLE_MULTIORG, 
                    src.GRD_DISTINCT, 
                    src.GRD_FILTERABLELIST, 
                    src.GRD_FILTERABLE_MULTIORG, 
                    src.GRD_GISDATAFILTER, 
                    src.GRD_GISWOFIELDMAP, 
                    src.GRD_GRIDID, 
                    src.GRD_GRIDNAME, 
                    src.GRD_GRIDTYPE, 
                    src.GRD_HINTS, 
                    src.GRD_KEYFIELDS, 
                    src.GRD_KEYFIELDS_MULTIORG, 
                    src.GRD_LASTSAVED, 
                    src.GRD_MAXGRIDCOST, 
                    src.GRD_MOBILE, 
                    src.GRD_NOSCREENDESIGNER, 
                    src.GRD_OPTIMIZERON, 
                    src.GRD_ORGCOLNAME, 
                    src.GRD_PORTLETFLAG, 
                    src.GRD_SECENTITY, 
                    src.GRD_SORTABLELIST, 
                    src.GRD_SORTABLE_MULTIORG, 
                    src.GRD_TAB, 
                    src.GRD_UNITS, 
                    src.GRD_UPDATECOUNT, 
                    src.GRD_UPDATED, 
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