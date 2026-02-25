create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PMFORECASTPARAMS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PMFORECASTPARAMS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PMFORECASTPARAMS as target using (SELECT * FROM (SELECT 
            strm.PFP_ACTDUEDT_BGCOLOR, 
            strm.PFP_BACKFILLING, 
            strm.PFP_CAL_DAY, 
            strm.PFP_CATEGORIES, 
            strm.PFP_CODE, 
            strm.PFP_COSTCODES, 
            strm.PFP_CREATED, 
            strm.PFP_CRITICALITIES, 
            strm.PFP_DAILY_DSG, 
            strm.PFP_DEFPARAM, 
            strm.PFP_ENABLECHILDEQUIPTAB, 
            strm.PFP_ENDDATE, 
            strm.PFP_EVENT_CLASSES, 
            strm.PFP_EVENT_ORG, 
            strm.PFP_FORECASTING, 
            strm.PFP_FORECASTPM_BGCOLOR, 
            strm.PFP_FORWARDFILLING, 
            strm.PFP_INC_CHILDREN, 
            strm.PFP_JOBTYPES, 
            strm.PFP_LASTSAVED, 
            strm.PFP_LKDPMDUEDT_TXTCOLOR, 
            strm.PFP_LOCATIONS, 
            strm.PFP_MIN_FREQ, 
            strm.PFP_MONTH_DSG, 
            strm.PFP_MRCS, 
            strm.PFP_NESTED, 
            strm.PFP_OBCLASSES, 
            strm.PFP_OBJECTS, 
            strm.PFP_OBTYPES, 
            strm.PFP_PAGESIZE, 
            strm.PFP_PARAMETER, 
            strm.PFP_PARENTTYPE, 
            strm.PFP_PERSONS, 
            strm.PFP_PMCLASSES, 
            strm.PFP_PMSCHEDULES, 
            strm.PFP_PRIORITIES, 
            strm.PFP_QTR_DSG, 
            strm.PFP_SCHEDGRPS, 
            strm.PFP_SCREENHSPLIT, 
            strm.PFP_SESSIONID, 
            strm.PFP_SESSION_APRV, 
            strm.PFP_STARTDATE, 
            strm.PFP_TOPLEVEL, 
            strm.PFP_TOPLEVEL_ORG, 
            strm.PFP_UPDATECOUNT, 
            strm.PFP_USER, 
            strm.PFP_WEEKEND_BGCOLOR, 
            strm.PFP_WEEK_DSG, 
            strm.PFP_WORKHOURS, 
            strm.PFP_WORKORDER_BGCOLOR, 
            strm.PFP_YEAR_DSG, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PFP_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMFORECASTPARAMS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PFP_CODE=src.PFP_CODE) OR (target.PFP_CODE IS NULL AND src.PFP_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PFP_ACTDUEDT_BGCOLOR=src.PFP_ACTDUEDT_BGCOLOR, 
                    target.PFP_BACKFILLING=src.PFP_BACKFILLING, 
                    target.PFP_CAL_DAY=src.PFP_CAL_DAY, 
                    target.PFP_CATEGORIES=src.PFP_CATEGORIES, 
                    target.PFP_CODE=src.PFP_CODE, 
                    target.PFP_COSTCODES=src.PFP_COSTCODES, 
                    target.PFP_CREATED=src.PFP_CREATED, 
                    target.PFP_CRITICALITIES=src.PFP_CRITICALITIES, 
                    target.PFP_DAILY_DSG=src.PFP_DAILY_DSG, 
                    target.PFP_DEFPARAM=src.PFP_DEFPARAM, 
                    target.PFP_ENABLECHILDEQUIPTAB=src.PFP_ENABLECHILDEQUIPTAB, 
                    target.PFP_ENDDATE=src.PFP_ENDDATE, 
                    target.PFP_EVENT_CLASSES=src.PFP_EVENT_CLASSES, 
                    target.PFP_EVENT_ORG=src.PFP_EVENT_ORG, 
                    target.PFP_FORECASTING=src.PFP_FORECASTING, 
                    target.PFP_FORECASTPM_BGCOLOR=src.PFP_FORECASTPM_BGCOLOR, 
                    target.PFP_FORWARDFILLING=src.PFP_FORWARDFILLING, 
                    target.PFP_INC_CHILDREN=src.PFP_INC_CHILDREN, 
                    target.PFP_JOBTYPES=src.PFP_JOBTYPES, 
                    target.PFP_LASTSAVED=src.PFP_LASTSAVED, 
                    target.PFP_LKDPMDUEDT_TXTCOLOR=src.PFP_LKDPMDUEDT_TXTCOLOR, 
                    target.PFP_LOCATIONS=src.PFP_LOCATIONS, 
                    target.PFP_MIN_FREQ=src.PFP_MIN_FREQ, 
                    target.PFP_MONTH_DSG=src.PFP_MONTH_DSG, 
                    target.PFP_MRCS=src.PFP_MRCS, 
                    target.PFP_NESTED=src.PFP_NESTED, 
                    target.PFP_OBCLASSES=src.PFP_OBCLASSES, 
                    target.PFP_OBJECTS=src.PFP_OBJECTS, 
                    target.PFP_OBTYPES=src.PFP_OBTYPES, 
                    target.PFP_PAGESIZE=src.PFP_PAGESIZE, 
                    target.PFP_PARAMETER=src.PFP_PARAMETER, 
                    target.PFP_PARENTTYPE=src.PFP_PARENTTYPE, 
                    target.PFP_PERSONS=src.PFP_PERSONS, 
                    target.PFP_PMCLASSES=src.PFP_PMCLASSES, 
                    target.PFP_PMSCHEDULES=src.PFP_PMSCHEDULES, 
                    target.PFP_PRIORITIES=src.PFP_PRIORITIES, 
                    target.PFP_QTR_DSG=src.PFP_QTR_DSG, 
                    target.PFP_SCHEDGRPS=src.PFP_SCHEDGRPS, 
                    target.PFP_SCREENHSPLIT=src.PFP_SCREENHSPLIT, 
                    target.PFP_SESSIONID=src.PFP_SESSIONID, 
                    target.PFP_SESSION_APRV=src.PFP_SESSION_APRV, 
                    target.PFP_STARTDATE=src.PFP_STARTDATE, 
                    target.PFP_TOPLEVEL=src.PFP_TOPLEVEL, 
                    target.PFP_TOPLEVEL_ORG=src.PFP_TOPLEVEL_ORG, 
                    target.PFP_UPDATECOUNT=src.PFP_UPDATECOUNT, 
                    target.PFP_USER=src.PFP_USER, 
                    target.PFP_WEEKEND_BGCOLOR=src.PFP_WEEKEND_BGCOLOR, 
                    target.PFP_WEEK_DSG=src.PFP_WEEK_DSG, 
                    target.PFP_WORKHOURS=src.PFP_WORKHOURS, 
                    target.PFP_WORKORDER_BGCOLOR=src.PFP_WORKORDER_BGCOLOR, 
                    target.PFP_YEAR_DSG=src.PFP_YEAR_DSG, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PFP_ACTDUEDT_BGCOLOR, 
                    PFP_BACKFILLING, 
                    PFP_CAL_DAY, 
                    PFP_CATEGORIES, 
                    PFP_CODE, 
                    PFP_COSTCODES, 
                    PFP_CREATED, 
                    PFP_CRITICALITIES, 
                    PFP_DAILY_DSG, 
                    PFP_DEFPARAM, 
                    PFP_ENABLECHILDEQUIPTAB, 
                    PFP_ENDDATE, 
                    PFP_EVENT_CLASSES, 
                    PFP_EVENT_ORG, 
                    PFP_FORECASTING, 
                    PFP_FORECASTPM_BGCOLOR, 
                    PFP_FORWARDFILLING, 
                    PFP_INC_CHILDREN, 
                    PFP_JOBTYPES, 
                    PFP_LASTSAVED, 
                    PFP_LKDPMDUEDT_TXTCOLOR, 
                    PFP_LOCATIONS, 
                    PFP_MIN_FREQ, 
                    PFP_MONTH_DSG, 
                    PFP_MRCS, 
                    PFP_NESTED, 
                    PFP_OBCLASSES, 
                    PFP_OBJECTS, 
                    PFP_OBTYPES, 
                    PFP_PAGESIZE, 
                    PFP_PARAMETER, 
                    PFP_PARENTTYPE, 
                    PFP_PERSONS, 
                    PFP_PMCLASSES, 
                    PFP_PMSCHEDULES, 
                    PFP_PRIORITIES, 
                    PFP_QTR_DSG, 
                    PFP_SCHEDGRPS, 
                    PFP_SCREENHSPLIT, 
                    PFP_SESSIONID, 
                    PFP_SESSION_APRV, 
                    PFP_STARTDATE, 
                    PFP_TOPLEVEL, 
                    PFP_TOPLEVEL_ORG, 
                    PFP_UPDATECOUNT, 
                    PFP_USER, 
                    PFP_WEEKEND_BGCOLOR, 
                    PFP_WEEK_DSG, 
                    PFP_WORKHOURS, 
                    PFP_WORKORDER_BGCOLOR, 
                    PFP_YEAR_DSG, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PFP_ACTDUEDT_BGCOLOR, 
                    src.PFP_BACKFILLING, 
                    src.PFP_CAL_DAY, 
                    src.PFP_CATEGORIES, 
                    src.PFP_CODE, 
                    src.PFP_COSTCODES, 
                    src.PFP_CREATED, 
                    src.PFP_CRITICALITIES, 
                    src.PFP_DAILY_DSG, 
                    src.PFP_DEFPARAM, 
                    src.PFP_ENABLECHILDEQUIPTAB, 
                    src.PFP_ENDDATE, 
                    src.PFP_EVENT_CLASSES, 
                    src.PFP_EVENT_ORG, 
                    src.PFP_FORECASTING, 
                    src.PFP_FORECASTPM_BGCOLOR, 
                    src.PFP_FORWARDFILLING, 
                    src.PFP_INC_CHILDREN, 
                    src.PFP_JOBTYPES, 
                    src.PFP_LASTSAVED, 
                    src.PFP_LKDPMDUEDT_TXTCOLOR, 
                    src.PFP_LOCATIONS, 
                    src.PFP_MIN_FREQ, 
                    src.PFP_MONTH_DSG, 
                    src.PFP_MRCS, 
                    src.PFP_NESTED, 
                    src.PFP_OBCLASSES, 
                    src.PFP_OBJECTS, 
                    src.PFP_OBTYPES, 
                    src.PFP_PAGESIZE, 
                    src.PFP_PARAMETER, 
                    src.PFP_PARENTTYPE, 
                    src.PFP_PERSONS, 
                    src.PFP_PMCLASSES, 
                    src.PFP_PMSCHEDULES, 
                    src.PFP_PRIORITIES, 
                    src.PFP_QTR_DSG, 
                    src.PFP_SCHEDGRPS, 
                    src.PFP_SCREENHSPLIT, 
                    src.PFP_SESSIONID, 
                    src.PFP_SESSION_APRV, 
                    src.PFP_STARTDATE, 
                    src.PFP_TOPLEVEL, 
                    src.PFP_TOPLEVEL_ORG, 
                    src.PFP_UPDATECOUNT, 
                    src.PFP_USER, 
                    src.PFP_WEEKEND_BGCOLOR, 
                    src.PFP_WEEK_DSG, 
                    src.PFP_WORKHOURS, 
                    src.PFP_WORKORDER_BGCOLOR, 
                    src.PFP_YEAR_DSG, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PMFORECASTPARAMS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PFP_ACTDUEDT_BGCOLOR, 
            strm.PFP_BACKFILLING, 
            strm.PFP_CAL_DAY, 
            strm.PFP_CATEGORIES, 
            strm.PFP_CODE, 
            strm.PFP_COSTCODES, 
            strm.PFP_CREATED, 
            strm.PFP_CRITICALITIES, 
            strm.PFP_DAILY_DSG, 
            strm.PFP_DEFPARAM, 
            strm.PFP_ENABLECHILDEQUIPTAB, 
            strm.PFP_ENDDATE, 
            strm.PFP_EVENT_CLASSES, 
            strm.PFP_EVENT_ORG, 
            strm.PFP_FORECASTING, 
            strm.PFP_FORECASTPM_BGCOLOR, 
            strm.PFP_FORWARDFILLING, 
            strm.PFP_INC_CHILDREN, 
            strm.PFP_JOBTYPES, 
            strm.PFP_LASTSAVED, 
            strm.PFP_LKDPMDUEDT_TXTCOLOR, 
            strm.PFP_LOCATIONS, 
            strm.PFP_MIN_FREQ, 
            strm.PFP_MONTH_DSG, 
            strm.PFP_MRCS, 
            strm.PFP_NESTED, 
            strm.PFP_OBCLASSES, 
            strm.PFP_OBJECTS, 
            strm.PFP_OBTYPES, 
            strm.PFP_PAGESIZE, 
            strm.PFP_PARAMETER, 
            strm.PFP_PARENTTYPE, 
            strm.PFP_PERSONS, 
            strm.PFP_PMCLASSES, 
            strm.PFP_PMSCHEDULES, 
            strm.PFP_PRIORITIES, 
            strm.PFP_QTR_DSG, 
            strm.PFP_SCHEDGRPS, 
            strm.PFP_SCREENHSPLIT, 
            strm.PFP_SESSIONID, 
            strm.PFP_SESSION_APRV, 
            strm.PFP_STARTDATE, 
            strm.PFP_TOPLEVEL, 
            strm.PFP_TOPLEVEL_ORG, 
            strm.PFP_UPDATECOUNT, 
            strm.PFP_USER, 
            strm.PFP_WEEKEND_BGCOLOR, 
            strm.PFP_WEEK_DSG, 
            strm.PFP_WORKHOURS, 
            strm.PFP_WORKORDER_BGCOLOR, 
            strm.PFP_YEAR_DSG, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PFP_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMFORECASTPARAMS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PFP_CODE=src.PFP_CODE) OR (target.PFP_CODE IS NULL AND src.PFP_CODE IS NULL)) 
                when matched then update set
                    target.PFP_ACTDUEDT_BGCOLOR=src.PFP_ACTDUEDT_BGCOLOR, 
                    target.PFP_BACKFILLING=src.PFP_BACKFILLING, 
                    target.PFP_CAL_DAY=src.PFP_CAL_DAY, 
                    target.PFP_CATEGORIES=src.PFP_CATEGORIES, 
                    target.PFP_CODE=src.PFP_CODE, 
                    target.PFP_COSTCODES=src.PFP_COSTCODES, 
                    target.PFP_CREATED=src.PFP_CREATED, 
                    target.PFP_CRITICALITIES=src.PFP_CRITICALITIES, 
                    target.PFP_DAILY_DSG=src.PFP_DAILY_DSG, 
                    target.PFP_DEFPARAM=src.PFP_DEFPARAM, 
                    target.PFP_ENABLECHILDEQUIPTAB=src.PFP_ENABLECHILDEQUIPTAB, 
                    target.PFP_ENDDATE=src.PFP_ENDDATE, 
                    target.PFP_EVENT_CLASSES=src.PFP_EVENT_CLASSES, 
                    target.PFP_EVENT_ORG=src.PFP_EVENT_ORG, 
                    target.PFP_FORECASTING=src.PFP_FORECASTING, 
                    target.PFP_FORECASTPM_BGCOLOR=src.PFP_FORECASTPM_BGCOLOR, 
                    target.PFP_FORWARDFILLING=src.PFP_FORWARDFILLING, 
                    target.PFP_INC_CHILDREN=src.PFP_INC_CHILDREN, 
                    target.PFP_JOBTYPES=src.PFP_JOBTYPES, 
                    target.PFP_LASTSAVED=src.PFP_LASTSAVED, 
                    target.PFP_LKDPMDUEDT_TXTCOLOR=src.PFP_LKDPMDUEDT_TXTCOLOR, 
                    target.PFP_LOCATIONS=src.PFP_LOCATIONS, 
                    target.PFP_MIN_FREQ=src.PFP_MIN_FREQ, 
                    target.PFP_MONTH_DSG=src.PFP_MONTH_DSG, 
                    target.PFP_MRCS=src.PFP_MRCS, 
                    target.PFP_NESTED=src.PFP_NESTED, 
                    target.PFP_OBCLASSES=src.PFP_OBCLASSES, 
                    target.PFP_OBJECTS=src.PFP_OBJECTS, 
                    target.PFP_OBTYPES=src.PFP_OBTYPES, 
                    target.PFP_PAGESIZE=src.PFP_PAGESIZE, 
                    target.PFP_PARAMETER=src.PFP_PARAMETER, 
                    target.PFP_PARENTTYPE=src.PFP_PARENTTYPE, 
                    target.PFP_PERSONS=src.PFP_PERSONS, 
                    target.PFP_PMCLASSES=src.PFP_PMCLASSES, 
                    target.PFP_PMSCHEDULES=src.PFP_PMSCHEDULES, 
                    target.PFP_PRIORITIES=src.PFP_PRIORITIES, 
                    target.PFP_QTR_DSG=src.PFP_QTR_DSG, 
                    target.PFP_SCHEDGRPS=src.PFP_SCHEDGRPS, 
                    target.PFP_SCREENHSPLIT=src.PFP_SCREENHSPLIT, 
                    target.PFP_SESSIONID=src.PFP_SESSIONID, 
                    target.PFP_SESSION_APRV=src.PFP_SESSION_APRV, 
                    target.PFP_STARTDATE=src.PFP_STARTDATE, 
                    target.PFP_TOPLEVEL=src.PFP_TOPLEVEL, 
                    target.PFP_TOPLEVEL_ORG=src.PFP_TOPLEVEL_ORG, 
                    target.PFP_UPDATECOUNT=src.PFP_UPDATECOUNT, 
                    target.PFP_USER=src.PFP_USER, 
                    target.PFP_WEEKEND_BGCOLOR=src.PFP_WEEKEND_BGCOLOR, 
                    target.PFP_WEEK_DSG=src.PFP_WEEK_DSG, 
                    target.PFP_WORKHOURS=src.PFP_WORKHOURS, 
                    target.PFP_WORKORDER_BGCOLOR=src.PFP_WORKORDER_BGCOLOR, 
                    target.PFP_YEAR_DSG=src.PFP_YEAR_DSG, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PFP_ACTDUEDT_BGCOLOR, PFP_BACKFILLING, PFP_CAL_DAY, PFP_CATEGORIES, PFP_CODE, PFP_COSTCODES, PFP_CREATED, PFP_CRITICALITIES, PFP_DAILY_DSG, PFP_DEFPARAM, PFP_ENABLECHILDEQUIPTAB, PFP_ENDDATE, PFP_EVENT_CLASSES, PFP_EVENT_ORG, PFP_FORECASTING, PFP_FORECASTPM_BGCOLOR, PFP_FORWARDFILLING, PFP_INC_CHILDREN, PFP_JOBTYPES, PFP_LASTSAVED, PFP_LKDPMDUEDT_TXTCOLOR, PFP_LOCATIONS, PFP_MIN_FREQ, PFP_MONTH_DSG, PFP_MRCS, PFP_NESTED, PFP_OBCLASSES, PFP_OBJECTS, PFP_OBTYPES, PFP_PAGESIZE, PFP_PARAMETER, PFP_PARENTTYPE, PFP_PERSONS, PFP_PMCLASSES, PFP_PMSCHEDULES, PFP_PRIORITIES, PFP_QTR_DSG, PFP_SCHEDGRPS, PFP_SCREENHSPLIT, PFP_SESSIONID, PFP_SESSION_APRV, PFP_STARTDATE, PFP_TOPLEVEL, PFP_TOPLEVEL_ORG, PFP_UPDATECOUNT, PFP_USER, PFP_WEEKEND_BGCOLOR, PFP_WEEK_DSG, PFP_WORKHOURS, PFP_WORKORDER_BGCOLOR, PFP_YEAR_DSG, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PFP_ACTDUEDT_BGCOLOR, 
                    src.PFP_BACKFILLING, 
                    src.PFP_CAL_DAY, 
                    src.PFP_CATEGORIES, 
                    src.PFP_CODE, 
                    src.PFP_COSTCODES, 
                    src.PFP_CREATED, 
                    src.PFP_CRITICALITIES, 
                    src.PFP_DAILY_DSG, 
                    src.PFP_DEFPARAM, 
                    src.PFP_ENABLECHILDEQUIPTAB, 
                    src.PFP_ENDDATE, 
                    src.PFP_EVENT_CLASSES, 
                    src.PFP_EVENT_ORG, 
                    src.PFP_FORECASTING, 
                    src.PFP_FORECASTPM_BGCOLOR, 
                    src.PFP_FORWARDFILLING, 
                    src.PFP_INC_CHILDREN, 
                    src.PFP_JOBTYPES, 
                    src.PFP_LASTSAVED, 
                    src.PFP_LKDPMDUEDT_TXTCOLOR, 
                    src.PFP_LOCATIONS, 
                    src.PFP_MIN_FREQ, 
                    src.PFP_MONTH_DSG, 
                    src.PFP_MRCS, 
                    src.PFP_NESTED, 
                    src.PFP_OBCLASSES, 
                    src.PFP_OBJECTS, 
                    src.PFP_OBTYPES, 
                    src.PFP_PAGESIZE, 
                    src.PFP_PARAMETER, 
                    src.PFP_PARENTTYPE, 
                    src.PFP_PERSONS, 
                    src.PFP_PMCLASSES, 
                    src.PFP_PMSCHEDULES, 
                    src.PFP_PRIORITIES, 
                    src.PFP_QTR_DSG, 
                    src.PFP_SCHEDGRPS, 
                    src.PFP_SCREENHSPLIT, 
                    src.PFP_SESSIONID, 
                    src.PFP_SESSION_APRV, 
                    src.PFP_STARTDATE, 
                    src.PFP_TOPLEVEL, 
                    src.PFP_TOPLEVEL_ORG, 
                    src.PFP_UPDATECOUNT, 
                    src.PFP_USER, 
                    src.PFP_WEEKEND_BGCOLOR, 
                    src.PFP_WEEK_DSG, 
                    src.PFP_WORKHOURS, 
                    src.PFP_WORKORDER_BGCOLOR, 
                    src.PFP_YEAR_DSG, 
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