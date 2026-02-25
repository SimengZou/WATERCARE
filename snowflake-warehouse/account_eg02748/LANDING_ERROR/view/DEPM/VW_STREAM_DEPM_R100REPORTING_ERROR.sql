CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_DEPM_R100REPORTING_ERROR AS SELECT src, 'DEPM_R100REPORTING' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:I_DueDate), '1900-01-01')) is null and 
                    src:I_DueDate is not null) THEN
                    'I_DueDate contains a non-timestamp value : \'' || AS_VARCHAR(src:I_DueDate) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:R_DueDate), '1900-01-01')) is null and 
                    src:R_DueDate is not null) THEN
                    'R_DueDate contains a non-timestamp value : \'' || AS_VARCHAR(src:R_DueDate) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:R_Mitig_DueDate), '1900-01-01')) is null and 
                    src:R_Mitig_DueDate is not null) THEN
                    'R_Mitig_DueDate contains a non-timestamp value : \'' || AS_VARCHAR(src:R_Mitig_DueDate) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TimeStamp), '1900-01-01')) is null and 
                    src:TimeStamp is not null) THEN
                    'TimeStamp contains a non-timestamp value : \'' || AS_VARCHAR(src:TimeStamp) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null) THEN
                'Timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:Timestamp) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:ProjectCode,
                src:R_ActionsCompleted,
                src:R_Status,
                src:ProjectName,
                src:R_Treatment,
                src:R_Mitig_Impact_Cost,
                src:R_Probability_Rank,
                src:R_Description,
                src:R_Risk_Rank,
                src:CategoryName,
                src:R_ActionsTotal,
                src:R_Owner,
                src:R_RiskNum,
                src:TimeStamp,
                src:I_Status,
                src:R_Mitig_Risk_Rank,
                src:WBSCode,
                src:R_Mitig_Impact_Rank,
                src:I_Impact_Rank,
                src:R_Title,
                src:R_Mitig_Risk_Cost,
                src:I_DataChanged,
                src:R_Mitig_DueDate,
                src:R_Mitig_Probability_Rank,
                src:CategoryCode,
                src:R_Impact_Rank,
                src:R_Risk_Cost,
                src:R_DueDate,
                src:R_Mitig_Probability_Rate,
                src:I_Description,
                src:R_Cause,
                src:R_Mitig_Cost,
                src:Version,
                src:R_InUse,
                src:RiskActionID,
                src:I_Owner,
                src:R_Mitig_InUse,
                src:I_Actions,
                src:I_Priority,
                src:R_Impact_Time,
                src:R_Probability_Rate,
                src:R_Mitig_Owner,
                src:WBSName,
                src:R_Mitig_NotInUse,
                src:I_Impact_Time,
                src:R_Mitig_Desc,
                src:RiskID,
                src:R_IsIssue,
                src:R_NotInUse,
                src:I_Impact_Cost,
                src:R_Impact_Cost,
                src:R_Status_Msg,
                src:R_Mitig_Impact_Time,
                src:Type,
                src:I_DueDate,
                src:R_Mitig_Status  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_R100REPORTING as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:I_DueDate), '1900-01-01')) is null and 
                    src:I_DueDate is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:R_DueDate), '1900-01-01')) is null and 
                    src:R_DueDate is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:R_Mitig_DueDate), '1900-01-01')) is null and 
                    src:R_Mitig_DueDate is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TimeStamp), '1900-01-01')) is null and 
                    src:TimeStamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:Timestamp), '1900-01-01')) is null and 
                src:Timestamp is not null)