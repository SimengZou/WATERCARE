CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_AMP AS SELECT
                        src:AMPBusinessArea::varchar AS AMPBusinessArea, 
                        src:AMPEMCodes::varchar AS AMPEMCodes, 
                        src:AMPMeas::varchar AS AMPMeas, 
                        src:AMPPeriod::varchar AS AMPPeriod, 
                        src:AMPProject::varchar AS AMPProject, 
                        src:AMPVersion::varchar AS AMPVersion, 
                        src:AMPYear::varchar AS AMPYear, 
                        src:AMPcodes::varchar AS AMPcodes, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Value::varchar AS Value, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AMPEMCodes,
                src:AMPYear,
                src:AMPProject,
                src:AMPMeas,
                src:AMPVersion,
                src:AMPPeriod,
                src:AMPBusinessArea  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_AMP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null