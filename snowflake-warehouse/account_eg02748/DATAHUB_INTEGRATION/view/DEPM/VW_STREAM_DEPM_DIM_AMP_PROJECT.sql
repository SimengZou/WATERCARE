CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_DIM_AMP_PROJECT AS SELECT
                        src:AMPCode::varchar AS AMPCode, 
                        src:Active::varchar AS Active, 
                        src:BusinessArea::varchar AS BusinessArea, 
                        src:EMCode::varchar AS EMCode, 
                        src:ElementName::varchar AS ElementName, 
                        src:Growth::varchar AS Growth, 
                        src:LevelofService::varchar AS LevelofService, 
                        src:Name::varchar AS Name, 
                        src:Parent::varchar AS Parent, 
                        src:ProgramManager::varchar AS ProgramManager, 
                        src:ProjectManager::varchar AS ProjectManager, 
                        src:Replacement::varchar AS Replacement, 
                        src:ShortName::varchar AS ShortName, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:Parent,
                src:ElementName  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_DIM_AMP_PROJECT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null