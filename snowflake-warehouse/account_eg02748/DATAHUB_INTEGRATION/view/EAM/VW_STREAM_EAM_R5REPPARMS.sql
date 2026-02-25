CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPPARMS AS SELECT
                        src:PMT_BOTTEXT::varchar AS PMT_BOTTEXT, 
                        src:PMT_DATATYPE::varchar AS PMT_DATATYPE, 
                        src:PMT_DEFAULT::varchar AS PMT_DEFAULT, 
                        src:PMT_EWSLOVDEF::varchar AS PMT_EWSLOVDEF, 
                        src:PMT_FIXED::varchar AS PMT_FIXED, 
                        src:PMT_FORMAT::varchar AS PMT_FORMAT, 
                        src:PMT_FUNCTION::varchar AS PMT_FUNCTION, 
                        src:PMT_LASTSAVED::datetime AS PMT_LASTSAVED, 
                        src:PMT_LENGTH::numeric(38, 10) AS PMT_LENGTH, 
                        src:PMT_LINE::numeric(38, 10) AS PMT_LINE, 
                        src:PMT_LOVFUNCTION::varchar AS PMT_LOVFUNCTION, 
                        src:PMT_MANDATORY::varchar AS PMT_MANDATORY, 
                        src:PMT_MEKEY::varchar AS PMT_MEKEY, 
                        src:PMT_OPTIONS::numeric(38, 10) AS PMT_OPTIONS, 
                        src:PMT_PARAMETER::varchar AS PMT_PARAMETER, 
                        src:PMT_PROPERTY::varchar AS PMT_PROPERTY, 
                        src:PMT_QUERY::varchar AS PMT_QUERY, 
                        src:PMT_REMEMBER::varchar AS PMT_REMEMBER, 
                        src:PMT_RENTITY::varchar AS PMT_RENTITY, 
                        src:PMT_RTYPE::varchar AS PMT_RTYPE, 
                        src:PMT_TEST::varchar AS PMT_TEST, 
                        src:PMT_UPDATECOUNT::numeric(38, 10) AS PMT_UPDATECOUNT, 
                        src:PMT_UPDATED::datetime AS PMT_UPDATED, 
                        src:PMT_UPPER::varchar AS PMT_UPPER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PMT_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PMT_FUNCTION,
                src:PMT_PARAMETER  ORDER BY 
            src:PMT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPPARMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PMT_LENGTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PMT_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PMT_OPTIONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PMT_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PMT_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is not null