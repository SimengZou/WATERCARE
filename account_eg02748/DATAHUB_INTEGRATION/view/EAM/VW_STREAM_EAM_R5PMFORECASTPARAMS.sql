CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMFORECASTPARAMS AS SELECT
                        src:PFP_ACTDUEDT_BGCOLOR::varchar AS PFP_ACTDUEDT_BGCOLOR, 
                        src:PFP_BACKFILLING::varchar AS PFP_BACKFILLING, 
                        src:PFP_CAL_DAY::varchar AS PFP_CAL_DAY, 
                        src:PFP_CATEGORIES::varchar AS PFP_CATEGORIES, 
                        src:PFP_CODE::varchar AS PFP_CODE, 
                        src:PFP_COSTCODES::varchar AS PFP_COSTCODES, 
                        src:PFP_CREATED::datetime AS PFP_CREATED, 
                        src:PFP_CRITICALITIES::varchar AS PFP_CRITICALITIES, 
                        src:PFP_DAILY_DSG::varchar AS PFP_DAILY_DSG, 
                        src:PFP_DEFPARAM::varchar AS PFP_DEFPARAM, 
                        src:PFP_ENABLECHILDEQUIPTAB::varchar AS PFP_ENABLECHILDEQUIPTAB, 
                        src:PFP_ENDDATE::datetime AS PFP_ENDDATE, 
                        src:PFP_EVENT_CLASSES::varchar AS PFP_EVENT_CLASSES, 
                        src:PFP_EVENT_ORG::varchar AS PFP_EVENT_ORG, 
                        src:PFP_FORECASTING::varchar AS PFP_FORECASTING, 
                        src:PFP_FORECASTPM_BGCOLOR::varchar AS PFP_FORECASTPM_BGCOLOR, 
                        src:PFP_FORWARDFILLING::varchar AS PFP_FORWARDFILLING, 
                        src:PFP_INC_CHILDREN::varchar AS PFP_INC_CHILDREN, 
                        src:PFP_JOBTYPES::varchar AS PFP_JOBTYPES, 
                        src:PFP_LASTSAVED::datetime AS PFP_LASTSAVED, 
                        src:PFP_LKDPMDUEDT_TXTCOLOR::varchar AS PFP_LKDPMDUEDT_TXTCOLOR, 
                        src:PFP_LOCATIONS::varchar AS PFP_LOCATIONS, 
                        src:PFP_MIN_FREQ::numeric(38, 10) AS PFP_MIN_FREQ, 
                        src:PFP_MONTH_DSG::varchar AS PFP_MONTH_DSG, 
                        src:PFP_MRCS::varchar AS PFP_MRCS, 
                        src:PFP_NESTED::varchar AS PFP_NESTED, 
                        src:PFP_OBCLASSES::varchar AS PFP_OBCLASSES, 
                        src:PFP_OBJECTS::varchar AS PFP_OBJECTS, 
                        src:PFP_OBTYPES::varchar AS PFP_OBTYPES, 
                        src:PFP_PAGESIZE::numeric(38, 10) AS PFP_PAGESIZE, 
                        src:PFP_PARAMETER::varchar AS PFP_PARAMETER, 
                        src:PFP_PARENTTYPE::varchar AS PFP_PARENTTYPE, 
                        src:PFP_PERSONS::varchar AS PFP_PERSONS, 
                        src:PFP_PMCLASSES::varchar AS PFP_PMCLASSES, 
                        src:PFP_PMSCHEDULES::varchar AS PFP_PMSCHEDULES, 
                        src:PFP_PRIORITIES::varchar AS PFP_PRIORITIES, 
                        src:PFP_QTR_DSG::varchar AS PFP_QTR_DSG, 
                        src:PFP_SCHEDGRPS::varchar AS PFP_SCHEDGRPS, 
                        src:PFP_SCREENHSPLIT::numeric(38, 10) AS PFP_SCREENHSPLIT, 
                        src:PFP_SESSIONID::numeric(38, 10) AS PFP_SESSIONID, 
                        src:PFP_SESSION_APRV::varchar AS PFP_SESSION_APRV, 
                        src:PFP_STARTDATE::datetime AS PFP_STARTDATE, 
                        src:PFP_TOPLEVEL::varchar AS PFP_TOPLEVEL, 
                        src:PFP_TOPLEVEL_ORG::varchar AS PFP_TOPLEVEL_ORG, 
                        src:PFP_UPDATECOUNT::numeric(38, 10) AS PFP_UPDATECOUNT, 
                        src:PFP_USER::varchar AS PFP_USER, 
                        src:PFP_WEEKEND_BGCOLOR::varchar AS PFP_WEEKEND_BGCOLOR, 
                        src:PFP_WEEK_DSG::varchar AS PFP_WEEK_DSG, 
                        src:PFP_WORKHOURS::numeric(38, 10) AS PFP_WORKHOURS, 
                        src:PFP_WORKORDER_BGCOLOR::varchar AS PFP_WORKORDER_BGCOLOR, 
                        src:PFP_YEAR_DSG::varchar AS PFP_YEAR_DSG, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PFP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PFP_CODE  ORDER BY 
            src:PFP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMFORECASTPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFP_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFP_ENDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_MIN_FREQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_PAGESIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_SCREENHSPLIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFP_STARTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PFP_WORKHOURS), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is not null