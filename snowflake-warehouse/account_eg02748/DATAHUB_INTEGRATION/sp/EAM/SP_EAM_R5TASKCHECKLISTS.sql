create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5TASKCHECKLISTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5TASKCHECKLISTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5TASKCHECKLISTS as target using (SELECT * FROM (SELECT 
            strm.TCH_ASMLEVEL, 
            strm.TCH_ASPECT, 
            strm.TCH_CODE, 
            strm.TCH_COLOR, 
            strm.TCH_COMPLEVEL, 
            strm.TCH_COMPLOCATION, 
            strm.TCH_CONDITION, 
            strm.TCH_CONDITION_OPTIONS, 
            strm.TCH_DESC, 
            strm.TCH_DIRECTION_OPTIONS, 
            strm.TCH_ENABLENONCONFORMITIES, 
            strm.TCH_EQUIPFILTER, 
            strm.TCH_FOLLOWUPJOB, 
            strm.TCH_FOLLOWUPTASK, 
            strm.TCH_FORMULA, 
            strm.TCH_FRACTIONSLIDERDIMENSIONS, 
            strm.TCH_GROUP_LABEL, 
            strm.TCH_HIDEFOLLOWUP, 
            strm.TCH_HIDELINEARFIELDS, 
            strm.TCH_LASTSAVED, 
            strm.TCH_MATLIST, 
            strm.TCH_MAXSLIDERVALUE, 
            strm.TCH_MEASUREMENTRESP, 
            strm.TCH_METRICFRACTIONSLIDER, 
            strm.TCH_MINSLIDERVALUE, 
            strm.TCH_NOTUSED, 
            strm.TCH_NOT_APPLICABLE_OPTIONS, 
            strm.TCH_OBJECTCATEGORY, 
            strm.TCH_OBJECTCLASS, 
            strm.TCH_OBJECTCLASS_ORG, 
            strm.TCH_OBJECTLEVEL, 
            strm.TCH_PART, 
            strm.TCH_PARTCATEGORY, 
            strm.TCH_PARTCLASS, 
            strm.TCH_PARTCLASS_ORG, 
            strm.TCH_PARTCONDITION, 
            strm.TCH_PART_ORG, 
            strm.TCH_POINTTYPE, 
            strm.TCH_POSSIBLEFINDINGS, 
            strm.TCH_REFERENCE, 
            strm.TCH_REPEATING, 
            strm.TCH_REQUIREDTOCLOSE, 
            strm.TCH_RESPONSIBILITY, 
            strm.TCH_SEQUENCE, 
            strm.TCH_SRCCALCULATEDVALUE, 
            strm.TCH_SYSLEVEL, 
            strm.TCH_TASK, 
            strm.TCH_TASKREV, 
            strm.TCH_TYPE, 
            strm.TCH_UOM, 
            strm.TCH_UPDATECOUNT, 
            strm.TCH_UPDATED, 
            strm.TCH_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TCH_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TASKCHECKLISTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TCH_CODE=src.TCH_CODE) OR (target.TCH_CODE IS NULL AND src.TCH_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.TCH_ASMLEVEL=src.TCH_ASMLEVEL, 
                    target.TCH_ASPECT=src.TCH_ASPECT, 
                    target.TCH_CODE=src.TCH_CODE, 
                    target.TCH_COLOR=src.TCH_COLOR, 
                    target.TCH_COMPLEVEL=src.TCH_COMPLEVEL, 
                    target.TCH_COMPLOCATION=src.TCH_COMPLOCATION, 
                    target.TCH_CONDITION=src.TCH_CONDITION, 
                    target.TCH_CONDITION_OPTIONS=src.TCH_CONDITION_OPTIONS, 
                    target.TCH_DESC=src.TCH_DESC, 
                    target.TCH_DIRECTION_OPTIONS=src.TCH_DIRECTION_OPTIONS, 
                    target.TCH_ENABLENONCONFORMITIES=src.TCH_ENABLENONCONFORMITIES, 
                    target.TCH_EQUIPFILTER=src.TCH_EQUIPFILTER, 
                    target.TCH_FOLLOWUPJOB=src.TCH_FOLLOWUPJOB, 
                    target.TCH_FOLLOWUPTASK=src.TCH_FOLLOWUPTASK, 
                    target.TCH_FORMULA=src.TCH_FORMULA, 
                    target.TCH_FRACTIONSLIDERDIMENSIONS=src.TCH_FRACTIONSLIDERDIMENSIONS, 
                    target.TCH_GROUP_LABEL=src.TCH_GROUP_LABEL, 
                    target.TCH_HIDEFOLLOWUP=src.TCH_HIDEFOLLOWUP, 
                    target.TCH_HIDELINEARFIELDS=src.TCH_HIDELINEARFIELDS, 
                    target.TCH_LASTSAVED=src.TCH_LASTSAVED, 
                    target.TCH_MATLIST=src.TCH_MATLIST, 
                    target.TCH_MAXSLIDERVALUE=src.TCH_MAXSLIDERVALUE, 
                    target.TCH_MEASUREMENTRESP=src.TCH_MEASUREMENTRESP, 
                    target.TCH_METRICFRACTIONSLIDER=src.TCH_METRICFRACTIONSLIDER, 
                    target.TCH_MINSLIDERVALUE=src.TCH_MINSLIDERVALUE, 
                    target.TCH_NOTUSED=src.TCH_NOTUSED, 
                    target.TCH_NOT_APPLICABLE_OPTIONS=src.TCH_NOT_APPLICABLE_OPTIONS, 
                    target.TCH_OBJECTCATEGORY=src.TCH_OBJECTCATEGORY, 
                    target.TCH_OBJECTCLASS=src.TCH_OBJECTCLASS, 
                    target.TCH_OBJECTCLASS_ORG=src.TCH_OBJECTCLASS_ORG, 
                    target.TCH_OBJECTLEVEL=src.TCH_OBJECTLEVEL, 
                    target.TCH_PART=src.TCH_PART, 
                    target.TCH_PARTCATEGORY=src.TCH_PARTCATEGORY, 
                    target.TCH_PARTCLASS=src.TCH_PARTCLASS, 
                    target.TCH_PARTCLASS_ORG=src.TCH_PARTCLASS_ORG, 
                    target.TCH_PARTCONDITION=src.TCH_PARTCONDITION, 
                    target.TCH_PART_ORG=src.TCH_PART_ORG, 
                    target.TCH_POINTTYPE=src.TCH_POINTTYPE, 
                    target.TCH_POSSIBLEFINDINGS=src.TCH_POSSIBLEFINDINGS, 
                    target.TCH_REFERENCE=src.TCH_REFERENCE, 
                    target.TCH_REPEATING=src.TCH_REPEATING, 
                    target.TCH_REQUIREDTOCLOSE=src.TCH_REQUIREDTOCLOSE, 
                    target.TCH_RESPONSIBILITY=src.TCH_RESPONSIBILITY, 
                    target.TCH_SEQUENCE=src.TCH_SEQUENCE, 
                    target.TCH_SRCCALCULATEDVALUE=src.TCH_SRCCALCULATEDVALUE, 
                    target.TCH_SYSLEVEL=src.TCH_SYSLEVEL, 
                    target.TCH_TASK=src.TCH_TASK, 
                    target.TCH_TASKREV=src.TCH_TASKREV, 
                    target.TCH_TYPE=src.TCH_TYPE, 
                    target.TCH_UOM=src.TCH_UOM, 
                    target.TCH_UPDATECOUNT=src.TCH_UPDATECOUNT, 
                    target.TCH_UPDATED=src.TCH_UPDATED, 
                    target.TCH_UPDATEDBY=src.TCH_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    TCH_ASMLEVEL, 
                    TCH_ASPECT, 
                    TCH_CODE, 
                    TCH_COLOR, 
                    TCH_COMPLEVEL, 
                    TCH_COMPLOCATION, 
                    TCH_CONDITION, 
                    TCH_CONDITION_OPTIONS, 
                    TCH_DESC, 
                    TCH_DIRECTION_OPTIONS, 
                    TCH_ENABLENONCONFORMITIES, 
                    TCH_EQUIPFILTER, 
                    TCH_FOLLOWUPJOB, 
                    TCH_FOLLOWUPTASK, 
                    TCH_FORMULA, 
                    TCH_FRACTIONSLIDERDIMENSIONS, 
                    TCH_GROUP_LABEL, 
                    TCH_HIDEFOLLOWUP, 
                    TCH_HIDELINEARFIELDS, 
                    TCH_LASTSAVED, 
                    TCH_MATLIST, 
                    TCH_MAXSLIDERVALUE, 
                    TCH_MEASUREMENTRESP, 
                    TCH_METRICFRACTIONSLIDER, 
                    TCH_MINSLIDERVALUE, 
                    TCH_NOTUSED, 
                    TCH_NOT_APPLICABLE_OPTIONS, 
                    TCH_OBJECTCATEGORY, 
                    TCH_OBJECTCLASS, 
                    TCH_OBJECTCLASS_ORG, 
                    TCH_OBJECTLEVEL, 
                    TCH_PART, 
                    TCH_PARTCATEGORY, 
                    TCH_PARTCLASS, 
                    TCH_PARTCLASS_ORG, 
                    TCH_PARTCONDITION, 
                    TCH_PART_ORG, 
                    TCH_POINTTYPE, 
                    TCH_POSSIBLEFINDINGS, 
                    TCH_REFERENCE, 
                    TCH_REPEATING, 
                    TCH_REQUIREDTOCLOSE, 
                    TCH_RESPONSIBILITY, 
                    TCH_SEQUENCE, 
                    TCH_SRCCALCULATEDVALUE, 
                    TCH_SYSLEVEL, 
                    TCH_TASK, 
                    TCH_TASKREV, 
                    TCH_TYPE, 
                    TCH_UOM, 
                    TCH_UPDATECOUNT, 
                    TCH_UPDATED, 
                    TCH_UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.TCH_ASMLEVEL, 
                    src.TCH_ASPECT, 
                    src.TCH_CODE, 
                    src.TCH_COLOR, 
                    src.TCH_COMPLEVEL, 
                    src.TCH_COMPLOCATION, 
                    src.TCH_CONDITION, 
                    src.TCH_CONDITION_OPTIONS, 
                    src.TCH_DESC, 
                    src.TCH_DIRECTION_OPTIONS, 
                    src.TCH_ENABLENONCONFORMITIES, 
                    src.TCH_EQUIPFILTER, 
                    src.TCH_FOLLOWUPJOB, 
                    src.TCH_FOLLOWUPTASK, 
                    src.TCH_FORMULA, 
                    src.TCH_FRACTIONSLIDERDIMENSIONS, 
                    src.TCH_GROUP_LABEL, 
                    src.TCH_HIDEFOLLOWUP, 
                    src.TCH_HIDELINEARFIELDS, 
                    src.TCH_LASTSAVED, 
                    src.TCH_MATLIST, 
                    src.TCH_MAXSLIDERVALUE, 
                    src.TCH_MEASUREMENTRESP, 
                    src.TCH_METRICFRACTIONSLIDER, 
                    src.TCH_MINSLIDERVALUE, 
                    src.TCH_NOTUSED, 
                    src.TCH_NOT_APPLICABLE_OPTIONS, 
                    src.TCH_OBJECTCATEGORY, 
                    src.TCH_OBJECTCLASS, 
                    src.TCH_OBJECTCLASS_ORG, 
                    src.TCH_OBJECTLEVEL, 
                    src.TCH_PART, 
                    src.TCH_PARTCATEGORY, 
                    src.TCH_PARTCLASS, 
                    src.TCH_PARTCLASS_ORG, 
                    src.TCH_PARTCONDITION, 
                    src.TCH_PART_ORG, 
                    src.TCH_POINTTYPE, 
                    src.TCH_POSSIBLEFINDINGS, 
                    src.TCH_REFERENCE, 
                    src.TCH_REPEATING, 
                    src.TCH_REQUIREDTOCLOSE, 
                    src.TCH_RESPONSIBILITY, 
                    src.TCH_SEQUENCE, 
                    src.TCH_SRCCALCULATEDVALUE, 
                    src.TCH_SYSLEVEL, 
                    src.TCH_TASK, 
                    src.TCH_TASKREV, 
                    src.TCH_TYPE, 
                    src.TCH_UOM, 
                    src.TCH_UPDATECOUNT, 
                    src.TCH_UPDATED, 
                    src.TCH_UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5TASKCHECKLISTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.TCH_ASMLEVEL, 
            strm.TCH_ASPECT, 
            strm.TCH_CODE, 
            strm.TCH_COLOR, 
            strm.TCH_COMPLEVEL, 
            strm.TCH_COMPLOCATION, 
            strm.TCH_CONDITION, 
            strm.TCH_CONDITION_OPTIONS, 
            strm.TCH_DESC, 
            strm.TCH_DIRECTION_OPTIONS, 
            strm.TCH_ENABLENONCONFORMITIES, 
            strm.TCH_EQUIPFILTER, 
            strm.TCH_FOLLOWUPJOB, 
            strm.TCH_FOLLOWUPTASK, 
            strm.TCH_FORMULA, 
            strm.TCH_FRACTIONSLIDERDIMENSIONS, 
            strm.TCH_GROUP_LABEL, 
            strm.TCH_HIDEFOLLOWUP, 
            strm.TCH_HIDELINEARFIELDS, 
            strm.TCH_LASTSAVED, 
            strm.TCH_MATLIST, 
            strm.TCH_MAXSLIDERVALUE, 
            strm.TCH_MEASUREMENTRESP, 
            strm.TCH_METRICFRACTIONSLIDER, 
            strm.TCH_MINSLIDERVALUE, 
            strm.TCH_NOTUSED, 
            strm.TCH_NOT_APPLICABLE_OPTIONS, 
            strm.TCH_OBJECTCATEGORY, 
            strm.TCH_OBJECTCLASS, 
            strm.TCH_OBJECTCLASS_ORG, 
            strm.TCH_OBJECTLEVEL, 
            strm.TCH_PART, 
            strm.TCH_PARTCATEGORY, 
            strm.TCH_PARTCLASS, 
            strm.TCH_PARTCLASS_ORG, 
            strm.TCH_PARTCONDITION, 
            strm.TCH_PART_ORG, 
            strm.TCH_POINTTYPE, 
            strm.TCH_POSSIBLEFINDINGS, 
            strm.TCH_REFERENCE, 
            strm.TCH_REPEATING, 
            strm.TCH_REQUIREDTOCLOSE, 
            strm.TCH_RESPONSIBILITY, 
            strm.TCH_SEQUENCE, 
            strm.TCH_SRCCALCULATEDVALUE, 
            strm.TCH_SYSLEVEL, 
            strm.TCH_TASK, 
            strm.TCH_TASKREV, 
            strm.TCH_TYPE, 
            strm.TCH_UOM, 
            strm.TCH_UPDATECOUNT, 
            strm.TCH_UPDATED, 
            strm.TCH_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TCH_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TASKCHECKLISTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TCH_CODE=src.TCH_CODE) OR (target.TCH_CODE IS NULL AND src.TCH_CODE IS NULL)) 
                when matched then update set
                    target.TCH_ASMLEVEL=src.TCH_ASMLEVEL, 
                    target.TCH_ASPECT=src.TCH_ASPECT, 
                    target.TCH_CODE=src.TCH_CODE, 
                    target.TCH_COLOR=src.TCH_COLOR, 
                    target.TCH_COMPLEVEL=src.TCH_COMPLEVEL, 
                    target.TCH_COMPLOCATION=src.TCH_COMPLOCATION, 
                    target.TCH_CONDITION=src.TCH_CONDITION, 
                    target.TCH_CONDITION_OPTIONS=src.TCH_CONDITION_OPTIONS, 
                    target.TCH_DESC=src.TCH_DESC, 
                    target.TCH_DIRECTION_OPTIONS=src.TCH_DIRECTION_OPTIONS, 
                    target.TCH_ENABLENONCONFORMITIES=src.TCH_ENABLENONCONFORMITIES, 
                    target.TCH_EQUIPFILTER=src.TCH_EQUIPFILTER, 
                    target.TCH_FOLLOWUPJOB=src.TCH_FOLLOWUPJOB, 
                    target.TCH_FOLLOWUPTASK=src.TCH_FOLLOWUPTASK, 
                    target.TCH_FORMULA=src.TCH_FORMULA, 
                    target.TCH_FRACTIONSLIDERDIMENSIONS=src.TCH_FRACTIONSLIDERDIMENSIONS, 
                    target.TCH_GROUP_LABEL=src.TCH_GROUP_LABEL, 
                    target.TCH_HIDEFOLLOWUP=src.TCH_HIDEFOLLOWUP, 
                    target.TCH_HIDELINEARFIELDS=src.TCH_HIDELINEARFIELDS, 
                    target.TCH_LASTSAVED=src.TCH_LASTSAVED, 
                    target.TCH_MATLIST=src.TCH_MATLIST, 
                    target.TCH_MAXSLIDERVALUE=src.TCH_MAXSLIDERVALUE, 
                    target.TCH_MEASUREMENTRESP=src.TCH_MEASUREMENTRESP, 
                    target.TCH_METRICFRACTIONSLIDER=src.TCH_METRICFRACTIONSLIDER, 
                    target.TCH_MINSLIDERVALUE=src.TCH_MINSLIDERVALUE, 
                    target.TCH_NOTUSED=src.TCH_NOTUSED, 
                    target.TCH_NOT_APPLICABLE_OPTIONS=src.TCH_NOT_APPLICABLE_OPTIONS, 
                    target.TCH_OBJECTCATEGORY=src.TCH_OBJECTCATEGORY, 
                    target.TCH_OBJECTCLASS=src.TCH_OBJECTCLASS, 
                    target.TCH_OBJECTCLASS_ORG=src.TCH_OBJECTCLASS_ORG, 
                    target.TCH_OBJECTLEVEL=src.TCH_OBJECTLEVEL, 
                    target.TCH_PART=src.TCH_PART, 
                    target.TCH_PARTCATEGORY=src.TCH_PARTCATEGORY, 
                    target.TCH_PARTCLASS=src.TCH_PARTCLASS, 
                    target.TCH_PARTCLASS_ORG=src.TCH_PARTCLASS_ORG, 
                    target.TCH_PARTCONDITION=src.TCH_PARTCONDITION, 
                    target.TCH_PART_ORG=src.TCH_PART_ORG, 
                    target.TCH_POINTTYPE=src.TCH_POINTTYPE, 
                    target.TCH_POSSIBLEFINDINGS=src.TCH_POSSIBLEFINDINGS, 
                    target.TCH_REFERENCE=src.TCH_REFERENCE, 
                    target.TCH_REPEATING=src.TCH_REPEATING, 
                    target.TCH_REQUIREDTOCLOSE=src.TCH_REQUIREDTOCLOSE, 
                    target.TCH_RESPONSIBILITY=src.TCH_RESPONSIBILITY, 
                    target.TCH_SEQUENCE=src.TCH_SEQUENCE, 
                    target.TCH_SRCCALCULATEDVALUE=src.TCH_SRCCALCULATEDVALUE, 
                    target.TCH_SYSLEVEL=src.TCH_SYSLEVEL, 
                    target.TCH_TASK=src.TCH_TASK, 
                    target.TCH_TASKREV=src.TCH_TASKREV, 
                    target.TCH_TYPE=src.TCH_TYPE, 
                    target.TCH_UOM=src.TCH_UOM, 
                    target.TCH_UPDATECOUNT=src.TCH_UPDATECOUNT, 
                    target.TCH_UPDATED=src.TCH_UPDATED, 
                    target.TCH_UPDATEDBY=src.TCH_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( TCH_ASMLEVEL, TCH_ASPECT, TCH_CODE, TCH_COLOR, TCH_COMPLEVEL, TCH_COMPLOCATION, TCH_CONDITION, TCH_CONDITION_OPTIONS, TCH_DESC, TCH_DIRECTION_OPTIONS, TCH_ENABLENONCONFORMITIES, TCH_EQUIPFILTER, TCH_FOLLOWUPJOB, TCH_FOLLOWUPTASK, TCH_FORMULA, TCH_FRACTIONSLIDERDIMENSIONS, TCH_GROUP_LABEL, TCH_HIDEFOLLOWUP, TCH_HIDELINEARFIELDS, TCH_LASTSAVED, TCH_MATLIST, TCH_MAXSLIDERVALUE, TCH_MEASUREMENTRESP, TCH_METRICFRACTIONSLIDER, TCH_MINSLIDERVALUE, TCH_NOTUSED, TCH_NOT_APPLICABLE_OPTIONS, TCH_OBJECTCATEGORY, TCH_OBJECTCLASS, TCH_OBJECTCLASS_ORG, TCH_OBJECTLEVEL, TCH_PART, TCH_PARTCATEGORY, TCH_PARTCLASS, TCH_PARTCLASS_ORG, TCH_PARTCONDITION, TCH_PART_ORG, TCH_POINTTYPE, TCH_POSSIBLEFINDINGS, TCH_REFERENCE, TCH_REPEATING, TCH_REQUIREDTOCLOSE, TCH_RESPONSIBILITY, TCH_SEQUENCE, TCH_SRCCALCULATEDVALUE, TCH_SYSLEVEL, TCH_TASK, TCH_TASKREV, TCH_TYPE, TCH_UOM, TCH_UPDATECOUNT, TCH_UPDATED, TCH_UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.TCH_ASMLEVEL, 
                    src.TCH_ASPECT, 
                    src.TCH_CODE, 
                    src.TCH_COLOR, 
                    src.TCH_COMPLEVEL, 
                    src.TCH_COMPLOCATION, 
                    src.TCH_CONDITION, 
                    src.TCH_CONDITION_OPTIONS, 
                    src.TCH_DESC, 
                    src.TCH_DIRECTION_OPTIONS, 
                    src.TCH_ENABLENONCONFORMITIES, 
                    src.TCH_EQUIPFILTER, 
                    src.TCH_FOLLOWUPJOB, 
                    src.TCH_FOLLOWUPTASK, 
                    src.TCH_FORMULA, 
                    src.TCH_FRACTIONSLIDERDIMENSIONS, 
                    src.TCH_GROUP_LABEL, 
                    src.TCH_HIDEFOLLOWUP, 
                    src.TCH_HIDELINEARFIELDS, 
                    src.TCH_LASTSAVED, 
                    src.TCH_MATLIST, 
                    src.TCH_MAXSLIDERVALUE, 
                    src.TCH_MEASUREMENTRESP, 
                    src.TCH_METRICFRACTIONSLIDER, 
                    src.TCH_MINSLIDERVALUE, 
                    src.TCH_NOTUSED, 
                    src.TCH_NOT_APPLICABLE_OPTIONS, 
                    src.TCH_OBJECTCATEGORY, 
                    src.TCH_OBJECTCLASS, 
                    src.TCH_OBJECTCLASS_ORG, 
                    src.TCH_OBJECTLEVEL, 
                    src.TCH_PART, 
                    src.TCH_PARTCATEGORY, 
                    src.TCH_PARTCLASS, 
                    src.TCH_PARTCLASS_ORG, 
                    src.TCH_PARTCONDITION, 
                    src.TCH_PART_ORG, 
                    src.TCH_POINTTYPE, 
                    src.TCH_POSSIBLEFINDINGS, 
                    src.TCH_REFERENCE, 
                    src.TCH_REPEATING, 
                    src.TCH_REQUIREDTOCLOSE, 
                    src.TCH_RESPONSIBILITY, 
                    src.TCH_SEQUENCE, 
                    src.TCH_SRCCALCULATEDVALUE, 
                    src.TCH_SYSLEVEL, 
                    src.TCH_TASK, 
                    src.TCH_TASKREV, 
                    src.TCH_TYPE, 
                    src.TCH_UOM, 
                    src.TCH_UPDATECOUNT, 
                    src.TCH_UPDATED, 
                    src.TCH_UPDATEDBY, 
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