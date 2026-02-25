create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5NONCONFORMITYSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5NONCONFORMITYSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5NONCONFORMITYSETUP as target using (SELECT * FROM (SELECT 
            strm.NCP_AUTOAPPROVESTATUS, 
            strm.NCP_AUTOSKIPSTATUS, 
            strm.NCP_COPYFROM, 
            strm.NCP_COPYIMPORTANCE, 
            strm.NCP_COPYINTENSITY, 
            strm.NCP_COPYNEXTINSPDATEOVERRIDE, 
            strm.NCP_COPYOBSERVATIONUDFS, 
            strm.NCP_COPYSEVERITY, 
            strm.NCP_COPYSIZE, 
            strm.NCP_CREATEFROMCHECKLIST, 
            strm.NCP_INCLUDECONDITION, 
            strm.NCP_LASTSAVED, 
            strm.NCP_MASSACKNOWLEDGEALLOWED, 
            strm.NCP_MERGERESTRICTIONS, 
            strm.NCP_ORG, 
            strm.NCP_PREVOBSCOUNT, 
            strm.NCP_PROTECTHEADER, 
            strm.NCP_PROTECTOBSDATAONHDR, 
            strm.NCP_REINSPECTSTATUS, 
            strm.NCP_SUPERSEDEDSTATUS, 
            strm.NCP_SYNCHRONIZE, 
            strm.NCP_UPDATECOUNT, 
            strm.NCP_UPDATED, 
            strm.NCP_UPDATEDBY, 
            strm.NCP_USEOBSSTATUS, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NCP_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5NONCONFORMITYSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NCP_ORG=src.NCP_ORG) OR (target.NCP_ORG IS NULL AND src.NCP_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.NCP_AUTOAPPROVESTATUS=src.NCP_AUTOAPPROVESTATUS, 
                    target.NCP_AUTOSKIPSTATUS=src.NCP_AUTOSKIPSTATUS, 
                    target.NCP_COPYFROM=src.NCP_COPYFROM, 
                    target.NCP_COPYIMPORTANCE=src.NCP_COPYIMPORTANCE, 
                    target.NCP_COPYINTENSITY=src.NCP_COPYINTENSITY, 
                    target.NCP_COPYNEXTINSPDATEOVERRIDE=src.NCP_COPYNEXTINSPDATEOVERRIDE, 
                    target.NCP_COPYOBSERVATIONUDFS=src.NCP_COPYOBSERVATIONUDFS, 
                    target.NCP_COPYSEVERITY=src.NCP_COPYSEVERITY, 
                    target.NCP_COPYSIZE=src.NCP_COPYSIZE, 
                    target.NCP_CREATEFROMCHECKLIST=src.NCP_CREATEFROMCHECKLIST, 
                    target.NCP_INCLUDECONDITION=src.NCP_INCLUDECONDITION, 
                    target.NCP_LASTSAVED=src.NCP_LASTSAVED, 
                    target.NCP_MASSACKNOWLEDGEALLOWED=src.NCP_MASSACKNOWLEDGEALLOWED, 
                    target.NCP_MERGERESTRICTIONS=src.NCP_MERGERESTRICTIONS, 
                    target.NCP_ORG=src.NCP_ORG, 
                    target.NCP_PREVOBSCOUNT=src.NCP_PREVOBSCOUNT, 
                    target.NCP_PROTECTHEADER=src.NCP_PROTECTHEADER, 
                    target.NCP_PROTECTOBSDATAONHDR=src.NCP_PROTECTOBSDATAONHDR, 
                    target.NCP_REINSPECTSTATUS=src.NCP_REINSPECTSTATUS, 
                    target.NCP_SUPERSEDEDSTATUS=src.NCP_SUPERSEDEDSTATUS, 
                    target.NCP_SYNCHRONIZE=src.NCP_SYNCHRONIZE, 
                    target.NCP_UPDATECOUNT=src.NCP_UPDATECOUNT, 
                    target.NCP_UPDATED=src.NCP_UPDATED, 
                    target.NCP_UPDATEDBY=src.NCP_UPDATEDBY, 
                    target.NCP_USEOBSSTATUS=src.NCP_USEOBSSTATUS, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    NCP_AUTOAPPROVESTATUS, 
                    NCP_AUTOSKIPSTATUS, 
                    NCP_COPYFROM, 
                    NCP_COPYIMPORTANCE, 
                    NCP_COPYINTENSITY, 
                    NCP_COPYNEXTINSPDATEOVERRIDE, 
                    NCP_COPYOBSERVATIONUDFS, 
                    NCP_COPYSEVERITY, 
                    NCP_COPYSIZE, 
                    NCP_CREATEFROMCHECKLIST, 
                    NCP_INCLUDECONDITION, 
                    NCP_LASTSAVED, 
                    NCP_MASSACKNOWLEDGEALLOWED, 
                    NCP_MERGERESTRICTIONS, 
                    NCP_ORG, 
                    NCP_PREVOBSCOUNT, 
                    NCP_PROTECTHEADER, 
                    NCP_PROTECTOBSDATAONHDR, 
                    NCP_REINSPECTSTATUS, 
                    NCP_SUPERSEDEDSTATUS, 
                    NCP_SYNCHRONIZE, 
                    NCP_UPDATECOUNT, 
                    NCP_UPDATED, 
                    NCP_UPDATEDBY, 
                    NCP_USEOBSSTATUS, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.NCP_AUTOAPPROVESTATUS, 
                    src.NCP_AUTOSKIPSTATUS, 
                    src.NCP_COPYFROM, 
                    src.NCP_COPYIMPORTANCE, 
                    src.NCP_COPYINTENSITY, 
                    src.NCP_COPYNEXTINSPDATEOVERRIDE, 
                    src.NCP_COPYOBSERVATIONUDFS, 
                    src.NCP_COPYSEVERITY, 
                    src.NCP_COPYSIZE, 
                    src.NCP_CREATEFROMCHECKLIST, 
                    src.NCP_INCLUDECONDITION, 
                    src.NCP_LASTSAVED, 
                    src.NCP_MASSACKNOWLEDGEALLOWED, 
                    src.NCP_MERGERESTRICTIONS, 
                    src.NCP_ORG, 
                    src.NCP_PREVOBSCOUNT, 
                    src.NCP_PROTECTHEADER, 
                    src.NCP_PROTECTOBSDATAONHDR, 
                    src.NCP_REINSPECTSTATUS, 
                    src.NCP_SUPERSEDEDSTATUS, 
                    src.NCP_SYNCHRONIZE, 
                    src.NCP_UPDATECOUNT, 
                    src.NCP_UPDATED, 
                    src.NCP_UPDATEDBY, 
                    src.NCP_USEOBSSTATUS, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5NONCONFORMITYSETUP_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.NCP_AUTOAPPROVESTATUS, 
            strm.NCP_AUTOSKIPSTATUS, 
            strm.NCP_COPYFROM, 
            strm.NCP_COPYIMPORTANCE, 
            strm.NCP_COPYINTENSITY, 
            strm.NCP_COPYNEXTINSPDATEOVERRIDE, 
            strm.NCP_COPYOBSERVATIONUDFS, 
            strm.NCP_COPYSEVERITY, 
            strm.NCP_COPYSIZE, 
            strm.NCP_CREATEFROMCHECKLIST, 
            strm.NCP_INCLUDECONDITION, 
            strm.NCP_LASTSAVED, 
            strm.NCP_MASSACKNOWLEDGEALLOWED, 
            strm.NCP_MERGERESTRICTIONS, 
            strm.NCP_ORG, 
            strm.NCP_PREVOBSCOUNT, 
            strm.NCP_PROTECTHEADER, 
            strm.NCP_PROTECTOBSDATAONHDR, 
            strm.NCP_REINSPECTSTATUS, 
            strm.NCP_SUPERSEDEDSTATUS, 
            strm.NCP_SYNCHRONIZE, 
            strm.NCP_UPDATECOUNT, 
            strm.NCP_UPDATED, 
            strm.NCP_UPDATEDBY, 
            strm.NCP_USEOBSSTATUS, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NCP_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5NONCONFORMITYSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NCP_ORG=src.NCP_ORG) OR (target.NCP_ORG IS NULL AND src.NCP_ORG IS NULL)) 
                when matched then update set
                    target.NCP_AUTOAPPROVESTATUS=src.NCP_AUTOAPPROVESTATUS, 
                    target.NCP_AUTOSKIPSTATUS=src.NCP_AUTOSKIPSTATUS, 
                    target.NCP_COPYFROM=src.NCP_COPYFROM, 
                    target.NCP_COPYIMPORTANCE=src.NCP_COPYIMPORTANCE, 
                    target.NCP_COPYINTENSITY=src.NCP_COPYINTENSITY, 
                    target.NCP_COPYNEXTINSPDATEOVERRIDE=src.NCP_COPYNEXTINSPDATEOVERRIDE, 
                    target.NCP_COPYOBSERVATIONUDFS=src.NCP_COPYOBSERVATIONUDFS, 
                    target.NCP_COPYSEVERITY=src.NCP_COPYSEVERITY, 
                    target.NCP_COPYSIZE=src.NCP_COPYSIZE, 
                    target.NCP_CREATEFROMCHECKLIST=src.NCP_CREATEFROMCHECKLIST, 
                    target.NCP_INCLUDECONDITION=src.NCP_INCLUDECONDITION, 
                    target.NCP_LASTSAVED=src.NCP_LASTSAVED, 
                    target.NCP_MASSACKNOWLEDGEALLOWED=src.NCP_MASSACKNOWLEDGEALLOWED, 
                    target.NCP_MERGERESTRICTIONS=src.NCP_MERGERESTRICTIONS, 
                    target.NCP_ORG=src.NCP_ORG, 
                    target.NCP_PREVOBSCOUNT=src.NCP_PREVOBSCOUNT, 
                    target.NCP_PROTECTHEADER=src.NCP_PROTECTHEADER, 
                    target.NCP_PROTECTOBSDATAONHDR=src.NCP_PROTECTOBSDATAONHDR, 
                    target.NCP_REINSPECTSTATUS=src.NCP_REINSPECTSTATUS, 
                    target.NCP_SUPERSEDEDSTATUS=src.NCP_SUPERSEDEDSTATUS, 
                    target.NCP_SYNCHRONIZE=src.NCP_SYNCHRONIZE, 
                    target.NCP_UPDATECOUNT=src.NCP_UPDATECOUNT, 
                    target.NCP_UPDATED=src.NCP_UPDATED, 
                    target.NCP_UPDATEDBY=src.NCP_UPDATEDBY, 
                    target.NCP_USEOBSSTATUS=src.NCP_USEOBSSTATUS, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( NCP_AUTOAPPROVESTATUS, NCP_AUTOSKIPSTATUS, NCP_COPYFROM, NCP_COPYIMPORTANCE, NCP_COPYINTENSITY, NCP_COPYNEXTINSPDATEOVERRIDE, NCP_COPYOBSERVATIONUDFS, NCP_COPYSEVERITY, NCP_COPYSIZE, NCP_CREATEFROMCHECKLIST, NCP_INCLUDECONDITION, NCP_LASTSAVED, NCP_MASSACKNOWLEDGEALLOWED, NCP_MERGERESTRICTIONS, NCP_ORG, NCP_PREVOBSCOUNT, NCP_PROTECTHEADER, NCP_PROTECTOBSDATAONHDR, NCP_REINSPECTSTATUS, NCP_SUPERSEDEDSTATUS, NCP_SYNCHRONIZE, NCP_UPDATECOUNT, NCP_UPDATED, NCP_UPDATEDBY, NCP_USEOBSSTATUS, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.NCP_AUTOAPPROVESTATUS, 
                    src.NCP_AUTOSKIPSTATUS, 
                    src.NCP_COPYFROM, 
                    src.NCP_COPYIMPORTANCE, 
                    src.NCP_COPYINTENSITY, 
                    src.NCP_COPYNEXTINSPDATEOVERRIDE, 
                    src.NCP_COPYOBSERVATIONUDFS, 
                    src.NCP_COPYSEVERITY, 
                    src.NCP_COPYSIZE, 
                    src.NCP_CREATEFROMCHECKLIST, 
                    src.NCP_INCLUDECONDITION, 
                    src.NCP_LASTSAVED, 
                    src.NCP_MASSACKNOWLEDGEALLOWED, 
                    src.NCP_MERGERESTRICTIONS, 
                    src.NCP_ORG, 
                    src.NCP_PREVOBSCOUNT, 
                    src.NCP_PROTECTHEADER, 
                    src.NCP_PROTECTOBSDATAONHDR, 
                    src.NCP_REINSPECTSTATUS, 
                    src.NCP_SUPERSEDEDSTATUS, 
                    src.NCP_SYNCHRONIZE, 
                    src.NCP_UPDATECOUNT, 
                    src.NCP_UPDATED, 
                    src.NCP_UPDATEDBY, 
                    src.NCP_USEOBSSTATUS, 
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