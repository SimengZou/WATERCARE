create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5UDFSCREENFIELDS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5UDFSCREENFIELDS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5UDFSCREENFIELDS as target using (SELECT * FROM (SELECT 
            strm.USF_COMPUTEDDATA, 
            strm.USF_CREATED, 
            strm.USF_CREATEDBY, 
            strm.USF_DESC, 
            strm.USF_DROPPDOWN, 
            strm.USF_FIELDLABEL, 
            strm.USF_FIELDNAME, 
            strm.USF_FIELDTYPE, 
            strm.USF_GENERATED, 
            strm.USF_LASTSAVED, 
            strm.USF_LOOKUPQUERY, 
            strm.USF_LOOKUPSOURCE, 
            strm.USF_MAXLENGTH, 
            strm.USF_NOTUSED, 
            strm.USF_NULLABLE, 
            strm.USF_PARENTFIELD, 
            strm.USF_PRECISION, 
            strm.USF_PRIMARY, 
            strm.USF_RETRIEVEDVALUELOOKUP, 
            strm.USF_SCALE, 
            strm.USF_SCREENNAME, 
            strm.USF_SEQUENCE, 
            strm.USF_UPDATECOUNT, 
            strm.USF_UPDATED, 
            strm.USF_UPDATEDBY, 
            strm.USF_UPPERCASE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.USF_SCREENNAME,
            strm.USF_FIELDNAME ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UDFSCREENFIELDS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.USF_SCREENNAME=src.USF_SCREENNAME) OR (target.USF_SCREENNAME IS NULL AND src.USF_SCREENNAME IS NULL)) AND
            ((target.USF_FIELDNAME=src.USF_FIELDNAME) OR (target.USF_FIELDNAME IS NULL AND src.USF_FIELDNAME IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.USF_COMPUTEDDATA=src.USF_COMPUTEDDATA, 
                    target.USF_CREATED=src.USF_CREATED, 
                    target.USF_CREATEDBY=src.USF_CREATEDBY, 
                    target.USF_DESC=src.USF_DESC, 
                    target.USF_DROPPDOWN=src.USF_DROPPDOWN, 
                    target.USF_FIELDLABEL=src.USF_FIELDLABEL, 
                    target.USF_FIELDNAME=src.USF_FIELDNAME, 
                    target.USF_FIELDTYPE=src.USF_FIELDTYPE, 
                    target.USF_GENERATED=src.USF_GENERATED, 
                    target.USF_LASTSAVED=src.USF_LASTSAVED, 
                    target.USF_LOOKUPQUERY=src.USF_LOOKUPQUERY, 
                    target.USF_LOOKUPSOURCE=src.USF_LOOKUPSOURCE, 
                    target.USF_MAXLENGTH=src.USF_MAXLENGTH, 
                    target.USF_NOTUSED=src.USF_NOTUSED, 
                    target.USF_NULLABLE=src.USF_NULLABLE, 
                    target.USF_PARENTFIELD=src.USF_PARENTFIELD, 
                    target.USF_PRECISION=src.USF_PRECISION, 
                    target.USF_PRIMARY=src.USF_PRIMARY, 
                    target.USF_RETRIEVEDVALUELOOKUP=src.USF_RETRIEVEDVALUELOOKUP, 
                    target.USF_SCALE=src.USF_SCALE, 
                    target.USF_SCREENNAME=src.USF_SCREENNAME, 
                    target.USF_SEQUENCE=src.USF_SEQUENCE, 
                    target.USF_UPDATECOUNT=src.USF_UPDATECOUNT, 
                    target.USF_UPDATED=src.USF_UPDATED, 
                    target.USF_UPDATEDBY=src.USF_UPDATEDBY, 
                    target.USF_UPPERCASE=src.USF_UPPERCASE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    USF_COMPUTEDDATA, 
                    USF_CREATED, 
                    USF_CREATEDBY, 
                    USF_DESC, 
                    USF_DROPPDOWN, 
                    USF_FIELDLABEL, 
                    USF_FIELDNAME, 
                    USF_FIELDTYPE, 
                    USF_GENERATED, 
                    USF_LASTSAVED, 
                    USF_LOOKUPQUERY, 
                    USF_LOOKUPSOURCE, 
                    USF_MAXLENGTH, 
                    USF_NOTUSED, 
                    USF_NULLABLE, 
                    USF_PARENTFIELD, 
                    USF_PRECISION, 
                    USF_PRIMARY, 
                    USF_RETRIEVEDVALUELOOKUP, 
                    USF_SCALE, 
                    USF_SCREENNAME, 
                    USF_SEQUENCE, 
                    USF_UPDATECOUNT, 
                    USF_UPDATED, 
                    USF_UPDATEDBY, 
                    USF_UPPERCASE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.USF_COMPUTEDDATA, 
                    src.USF_CREATED, 
                    src.USF_CREATEDBY, 
                    src.USF_DESC, 
                    src.USF_DROPPDOWN, 
                    src.USF_FIELDLABEL, 
                    src.USF_FIELDNAME, 
                    src.USF_FIELDTYPE, 
                    src.USF_GENERATED, 
                    src.USF_LASTSAVED, 
                    src.USF_LOOKUPQUERY, 
                    src.USF_LOOKUPSOURCE, 
                    src.USF_MAXLENGTH, 
                    src.USF_NOTUSED, 
                    src.USF_NULLABLE, 
                    src.USF_PARENTFIELD, 
                    src.USF_PRECISION, 
                    src.USF_PRIMARY, 
                    src.USF_RETRIEVEDVALUELOOKUP, 
                    src.USF_SCALE, 
                    src.USF_SCREENNAME, 
                    src.USF_SEQUENCE, 
                    src.USF_UPDATECOUNT, 
                    src.USF_UPDATED, 
                    src.USF_UPDATEDBY, 
                    src.USF_UPPERCASE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5UDFSCREENFIELDS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.USF_COMPUTEDDATA, 
            strm.USF_CREATED, 
            strm.USF_CREATEDBY, 
            strm.USF_DESC, 
            strm.USF_DROPPDOWN, 
            strm.USF_FIELDLABEL, 
            strm.USF_FIELDNAME, 
            strm.USF_FIELDTYPE, 
            strm.USF_GENERATED, 
            strm.USF_LASTSAVED, 
            strm.USF_LOOKUPQUERY, 
            strm.USF_LOOKUPSOURCE, 
            strm.USF_MAXLENGTH, 
            strm.USF_NOTUSED, 
            strm.USF_NULLABLE, 
            strm.USF_PARENTFIELD, 
            strm.USF_PRECISION, 
            strm.USF_PRIMARY, 
            strm.USF_RETRIEVEDVALUELOOKUP, 
            strm.USF_SCALE, 
            strm.USF_SCREENNAME, 
            strm.USF_SEQUENCE, 
            strm.USF_UPDATECOUNT, 
            strm.USF_UPDATED, 
            strm.USF_UPDATEDBY, 
            strm.USF_UPPERCASE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.USF_SCREENNAME,
            strm.USF_FIELDNAME ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UDFSCREENFIELDS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.USF_SCREENNAME=src.USF_SCREENNAME) OR (target.USF_SCREENNAME IS NULL AND src.USF_SCREENNAME IS NULL)) AND
            ((target.USF_FIELDNAME=src.USF_FIELDNAME) OR (target.USF_FIELDNAME IS NULL AND src.USF_FIELDNAME IS NULL)) 
                when matched then update set
                    target.USF_COMPUTEDDATA=src.USF_COMPUTEDDATA, 
                    target.USF_CREATED=src.USF_CREATED, 
                    target.USF_CREATEDBY=src.USF_CREATEDBY, 
                    target.USF_DESC=src.USF_DESC, 
                    target.USF_DROPPDOWN=src.USF_DROPPDOWN, 
                    target.USF_FIELDLABEL=src.USF_FIELDLABEL, 
                    target.USF_FIELDNAME=src.USF_FIELDNAME, 
                    target.USF_FIELDTYPE=src.USF_FIELDTYPE, 
                    target.USF_GENERATED=src.USF_GENERATED, 
                    target.USF_LASTSAVED=src.USF_LASTSAVED, 
                    target.USF_LOOKUPQUERY=src.USF_LOOKUPQUERY, 
                    target.USF_LOOKUPSOURCE=src.USF_LOOKUPSOURCE, 
                    target.USF_MAXLENGTH=src.USF_MAXLENGTH, 
                    target.USF_NOTUSED=src.USF_NOTUSED, 
                    target.USF_NULLABLE=src.USF_NULLABLE, 
                    target.USF_PARENTFIELD=src.USF_PARENTFIELD, 
                    target.USF_PRECISION=src.USF_PRECISION, 
                    target.USF_PRIMARY=src.USF_PRIMARY, 
                    target.USF_RETRIEVEDVALUELOOKUP=src.USF_RETRIEVEDVALUELOOKUP, 
                    target.USF_SCALE=src.USF_SCALE, 
                    target.USF_SCREENNAME=src.USF_SCREENNAME, 
                    target.USF_SEQUENCE=src.USF_SEQUENCE, 
                    target.USF_UPDATECOUNT=src.USF_UPDATECOUNT, 
                    target.USF_UPDATED=src.USF_UPDATED, 
                    target.USF_UPDATEDBY=src.USF_UPDATEDBY, 
                    target.USF_UPPERCASE=src.USF_UPPERCASE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( USF_COMPUTEDDATA, USF_CREATED, USF_CREATEDBY, USF_DESC, USF_DROPPDOWN, USF_FIELDLABEL, USF_FIELDNAME, USF_FIELDTYPE, USF_GENERATED, USF_LASTSAVED, USF_LOOKUPQUERY, USF_LOOKUPSOURCE, USF_MAXLENGTH, USF_NOTUSED, USF_NULLABLE, USF_PARENTFIELD, USF_PRECISION, USF_PRIMARY, USF_RETRIEVEDVALUELOOKUP, USF_SCALE, USF_SCREENNAME, USF_SEQUENCE, USF_UPDATECOUNT, USF_UPDATED, USF_UPDATEDBY, USF_UPPERCASE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.USF_COMPUTEDDATA, 
                    src.USF_CREATED, 
                    src.USF_CREATEDBY, 
                    src.USF_DESC, 
                    src.USF_DROPPDOWN, 
                    src.USF_FIELDLABEL, 
                    src.USF_FIELDNAME, 
                    src.USF_FIELDTYPE, 
                    src.USF_GENERATED, 
                    src.USF_LASTSAVED, 
                    src.USF_LOOKUPQUERY, 
                    src.USF_LOOKUPSOURCE, 
                    src.USF_MAXLENGTH, 
                    src.USF_NOTUSED, 
                    src.USF_NULLABLE, 
                    src.USF_PARENTFIELD, 
                    src.USF_PRECISION, 
                    src.USF_PRIMARY, 
                    src.USF_RETRIEVEDVALUELOOKUP, 
                    src.USF_SCALE, 
                    src.USF_SCREENNAME, 
                    src.USF_SEQUENCE, 
                    src.USF_UPDATECOUNT, 
                    src.USF_UPDATED, 
                    src.USF_UPDATEDBY, 
                    src.USF_UPPERCASE, 
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