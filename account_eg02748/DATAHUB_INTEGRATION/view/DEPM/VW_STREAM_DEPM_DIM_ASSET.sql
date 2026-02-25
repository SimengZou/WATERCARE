CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_DIM_ASSET AS SELECT
                        src:AcqType::varchar AS AcqType, 
                        src:Area::varchar AS Area, 
                        src:AsBuilt::varchar AS AsBuilt, 
                        src:CostCentre::varchar AS CostCentre, 
                        src:ElementName::varchar AS ElementName, 
                        src:InstallDate::varchar AS InstallDate, 
                        src:Life::varchar AS Life, 
                        src:LongName::varchar AS LongName, 
                        src:Model::varchar AS Model, 
                        src:Name::varchar AS Name, 
                        src:Project::varchar AS Project, 
                        src:Serial::varchar AS Serial, 
                        src:ServiceStatus::varchar AS ServiceStatus, 
                        src:SubArea::varchar AS SubArea, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:TypeCode::varchar AS TypeCode, 
                        src:TypeDesc::varchar AS TypeDesc, 
                        src:UnitType::varchar AS UnitType, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:Life,
                src:CostCentre,
                src:Serial,
                src:ElementName,
                src:SubArea,
                src:Name,
                src:AcqType,
                src:LongName,
                src:Model,
                src:UnitType,
                src:InstallDate,
                src:ServiceStatus,
                src:TypeDesc,
                src:Area,
                src:TypeCode,
                src:AsBuilt,
                src:Project  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_DIM_ASSET as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null