create or replace procedure DATAHUB_LOGGING.SP_PIPE_AND_TASK_NOTIFICATION()
     returns varchar not null
                  language javascript
                  as
                  $$
//  Variables
	var result = ""; // return status of this proc call;
	var email_address = "alexandre.paquetfays@water.co.nz, manu.maxi@water.co.nz";
    var subject = "Task and pipe monitoring";
    
	try {
//  Step 1.
//  Check if we have error
		sql_command = "SELECT count(1) FROM DATAHUB_LOGGING.VW_TASK_AND_PIPE_MONITORING WHERE ERROR_MESSAGE not like '%has become stale.%';";
		var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
		var rs = sqlStmt.execute();
		
		if (rs >= 1) then
        {
		    sql_command = "call system$send_email('SN_NOTIFICATION', '" + email_address + "','" + subject + "', 'This list of task are failing : \r\n' || (SELECT LISTAGG(EXEC_TIME || '  - ' || TYPE || '  - ' || OBJECT_NAME || ' :\r\n' || ERROR_MESSAGE || '\r\n\r\n') FROM DATAHUB_LOGGING.VW_TASK_AND_PIPE_MONITORING WHERE ERROR_MESSAGE not like '%has become stale.%' ORDER BY TYPE DESC, EXEC_TIME DESC));";
		    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
		    var rs = sqlStmt.execute();
        }
		
		result="Succes"
	}       
	catch (err)  {
        sql_command = "call system$send_email('SN_NOTIFICATION', '" + email_address + "','" + subject + "'', 'The monitoring tasks and pipes failed'))";
		var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
		var rs = sqlStmt.execute();
		
		result="Error"
    }

	return result;
$$;