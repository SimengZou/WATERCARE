CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_DIM_PROJECT AS SELECT
                        src:ElementName::varchar AS ElementName, 
                        src:HLevel::varchar AS HLevel, 
                        src:LongName::varchar AS LongName, 
                        src:Name::varchar AS Name, 
                        src:Parent::varchar AS Parent, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Type::varchar AS Type, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HLevel,
                src:ElementName,
                src:Parent,
                src:Name,
                src:LongName,
                src:Type  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_DIM_PROJECT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null