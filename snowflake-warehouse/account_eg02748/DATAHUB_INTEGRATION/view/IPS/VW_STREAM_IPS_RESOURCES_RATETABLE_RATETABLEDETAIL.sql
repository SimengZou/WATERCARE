CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_RATETABLE_RATETABLEDETAIL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BRANCHKEY::integer AS BRANCHKEY, 
                        src:CONVERSIONFACTOR::numeric(38, 10) AS CONVERSIONFACTOR, 
                        src:CYCLEDAYS::integer AS CYCLEDAYS, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFECTIVEDATE::datetime AS EFFECTIVEDATE, 
                        src:EFFECTIVETODATE::datetime AS EFFECTIVETODATE, 
                        src:FINALCYCLEDAYS::integer AS FINALCYCLEDAYS, 
                        src:FORMULAKEY::integer AS FORMULAKEY, 
                        src:ISRATECHANGEPRORATIONDISABLED::varchar AS ISRATECHANGEPRORATIONDISABLED, 
                        src:MAXIMUMCHARGE::numeric(38, 10) AS MAXIMUMCHARGE, 
                        src:MINIMUMCHARGE::numeric(38, 10) AS MINIMUMCHARGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONIKER::varchar AS MONIKER, 
                        src:NODENAME::varchar AS NODENAME, 
                        src:NODESOURCE::varchar AS NODESOURCE, 
                        src:NUMBEROFSTEPS::integer AS NUMBEROFSTEPS, 
                        src:PARENTVALUE::integer AS PARENTVALUE, 
                        src:PRORATEBASEFLAG::varchar AS PRORATEBASEFLAG, 
                        src:PRORATECYCLEBILLS::varchar AS PRORATECYCLEBILLS, 
                        src:PRORATESTEPSFLAG::varchar AS PRORATESTEPSFLAG, 
                        src:RANGEVALUE::varchar AS RANGEVALUE, 
                        src:RATEFORSTEP1::numeric(38, 10) AS RATEFORSTEP1, 
                        src:RATEFORSTEP10::numeric(38, 10) AS RATEFORSTEP10, 
                        src:RATEFORSTEP2::numeric(38, 10) AS RATEFORSTEP2, 
                        src:RATEFORSTEP3::numeric(38, 10) AS RATEFORSTEP3, 
                        src:RATEFORSTEP4::numeric(38, 10) AS RATEFORSTEP4, 
                        src:RATEFORSTEP5::numeric(38, 10) AS RATEFORSTEP5, 
                        src:RATEFORSTEP6::numeric(38, 10) AS RATEFORSTEP6, 
                        src:RATEFORSTEP7::numeric(38, 10) AS RATEFORSTEP7, 
                        src:RATEFORSTEP8::numeric(38, 10) AS RATEFORSTEP8, 
                        src:RATEFORSTEP9::numeric(38, 10) AS RATEFORSTEP9, 
                        src:RATEPERUNIT::integer AS RATEPERUNIT, 
                        src:RATESTEP1::numeric(38, 10) AS RATESTEP1, 
                        src:RATESTEP10::numeric(38, 10) AS RATESTEP10, 
                        src:RATESTEP2::numeric(38, 10) AS RATESTEP2, 
                        src:RATESTEP3::numeric(38, 10) AS RATESTEP3, 
                        src:RATESTEP4::numeric(38, 10) AS RATESTEP4, 
                        src:RATESTEP5::numeric(38, 10) AS RATESTEP5, 
                        src:RATESTEP6::numeric(38, 10) AS RATESTEP6, 
                        src:RATESTEP7::numeric(38, 10) AS RATESTEP7, 
                        src:RATESTEP8::numeric(38, 10) AS RATESTEP8, 
                        src:RATESTEP9::numeric(38, 10) AS RATESTEP9, 
                        src:RATETABLEDETAILKEY::integer AS RATETABLEDETAILKEY, 
                        src:RATETABLESETUPKEY::integer AS RATETABLESETUPKEY, 
                        src:RATETYPE::integer AS RATETYPE, 
                        src:RECURRINGFLAG::varchar AS RECURRINGFLAG, 
                        src:ROUNDPRORATESTEPS::varchar AS ROUNDPRORATESTEPS, 
                        src:ROUNDUP::varchar AS ROUNDUP, 
                        src:SORTORDER::integer AS SORTORDER, 
                        src:UOM::varchar AS UOM, 
                        src:USEFORPRORATING::integer AS USEFORPRORATING, 
                        src:USEOVERAGE::varchar AS USEOVERAGE, 
                        src:VALUE::numeric(38, 10) AS VALUE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
                                        
                src:RATETABLEDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_RATETABLEDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BRANCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONVERSIONFACTOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVETODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FINALCYCLEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEFORSTEP9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATEPERUNIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATESTEP9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATETABLESETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATETYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SORTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null