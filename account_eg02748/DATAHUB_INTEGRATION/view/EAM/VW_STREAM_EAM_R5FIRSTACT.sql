CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT AS SELECT
                        src:ACT_ACT::numeric(38, 10) AS ACT_ACT, 
                        src:ACT_ASMLEVEL::varchar AS ACT_ASMLEVEL, 
                        src:ACT_COMPLEVEL::varchar AS ACT_COMPLEVEL, 
                        src:ACT_COMPLOCATION::varchar AS ACT_COMPLOCATION, 
                        src:ACT_DURATION::numeric(38, 10) AS ACT_DURATION, 
                        src:ACT_EST::numeric(38, 10) AS ACT_EST, 
                        src:ACT_EVENT::varchar AS ACT_EVENT, 
                        src:ACT_LASTSAVED::datetime AS ACT_LASTSAVED, 
                        src:ACT_MANUFACT::varchar AS ACT_MANUFACT, 
                        src:ACT_MATLIST::varchar AS ACT_MATLIST, 
                        src:ACT_MULTIPLETRADES::varchar AS ACT_MULTIPLETRADES, 
                        src:ACT_PERSONS::numeric(38, 10) AS ACT_PERSONS, 
                        src:ACT_REM::numeric(38, 10) AS ACT_REM, 
                        src:ACT_RPC::varchar AS ACT_RPC, 
                        src:ACT_START::datetime AS ACT_START, 
                        src:ACT_SYSLEVEL::varchar AS ACT_SYSLEVEL, 
                        src:ACT_TASK::varchar AS ACT_TASK, 
                        src:ACT_TPF::varchar AS ACT_TPF, 
                        src:ACT_TRADE::varchar AS ACT_TRADE, 
                        src:ACT_WAP::varchar AS ACT_WAP, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ACT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ACT_EVENT  ORDER BY 
            src:ACT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FIRSTACT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_ACT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_EST), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_PERSONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACT_REM), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_START), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACT_LASTSAVED), '1900-01-01')) is not null