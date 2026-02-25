CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_LABOUR AS SELECT
                        src:"Account"::varchar AS "Account", 
                        src:Analysis01::varchar AS Analysis01, 
                        src:Analysis02::varchar AS Analysis02, 
                        src:Analysis03::varchar AS Analysis03, 
                        src:Analysis04::varchar AS Analysis04, 
                        src:Analysis05::varchar AS Analysis05, 
                        src:Currency::varchar AS Currency, 
                        src:LabourEmployeeType::varchar AS LabourEmployeeType, 
                        src:LabourLineNo::varchar AS LabourLineNo, 
                        src:LabourMeas::varchar AS LabourMeas, 
                        src:LabourTrxType::varchar AS LabourTrxType, 
                        src:MeasureType::varchar AS MeasureType, 
                        src:Period::varchar AS Period, 
                        src:Position::varchar AS Position, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Value::varchar AS Value, 
                        src:Version::varchar AS Version, 
                        src:Year::varchar AS Year, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LabourLineNo,
                src:Version,
                src:Year,
                src:Analysis05,
                src:MeasureType,
                src:Analysis01,
                src:Currency,
                src:Analysis04,
                src:LabourMeas,
                src:LabourEmployeeType,
                src:Analysis02,
                src:LabourTrxType,
                src:Period,
                src:Analysis03,
                src:Account,
                src:Position  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_LABOUR as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null