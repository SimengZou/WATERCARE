CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BASEBILLABLEUSAGE::numeric(38, 10) AS BASEBILLABLEUSAGE, 
                        src:CALCULATIONDETAILKEY::integer AS CALCULATIONDETAILKEY, 
                        src:CALCULATIONTABLE::integer AS CALCULATIONTABLE, 
                        src:CONVERTEDUNITS::numeric(38, 10) AS CONVERTEDUNITS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DAYSBETWEENREADS::integer AS DAYSBETWEENREADS, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EFFTODATE::datetime AS EFFTODATE, 
                        src:FULLAMT::numeric(38, 10) AS FULLAMT, 
                        src:ISPRORATESTEPSRATECHANGE::varchar AS ISPRORATESTEPSRATECHANGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMBEROFSTEPS::integer AS NUMBEROFSTEPS, 
                        src:PRORATEAMT::numeric(38, 10) AS PRORATEAMT, 
                        src:RATETABLEDETAILKEY::integer AS RATETABLEDETAILKEY, 
                        src:RTCHG1::numeric(38, 10) AS RTCHG1, 
                        src:RTCHG10::numeric(38, 10) AS RTCHG10, 
                        src:RTCHG2::numeric(38, 10) AS RTCHG2, 
                        src:RTCHG3::numeric(38, 10) AS RTCHG3, 
                        src:RTCHG4::numeric(38, 10) AS RTCHG4, 
                        src:RTCHG5::numeric(38, 10) AS RTCHG5, 
                        src:RTCHG6::numeric(38, 10) AS RTCHG6, 
                        src:RTCHG7::numeric(38, 10) AS RTCHG7, 
                        src:RTCHG8::numeric(38, 10) AS RTCHG8, 
                        src:RTCHG9::numeric(38, 10) AS RTCHG9, 
                        src:RTMAXCHG::numeric(38, 10) AS RTMAXCHG, 
                        src:RTMINCHG::numeric(38, 10) AS RTMINCHG, 
                        src:RTPRORATESTEP1::numeric(38, 10) AS RTPRORATESTEP1, 
                        src:RTPRORATESTEP10::numeric(38, 10) AS RTPRORATESTEP10, 
                        src:RTPRORATESTEP2::numeric(38, 10) AS RTPRORATESTEP2, 
                        src:RTPRORATESTEP3::numeric(38, 10) AS RTPRORATESTEP3, 
                        src:RTPRORATESTEP4::numeric(38, 10) AS RTPRORATESTEP4, 
                        src:RTPRORATESTEP5::numeric(38, 10) AS RTPRORATESTEP5, 
                        src:RTPRORATESTEP6::numeric(38, 10) AS RTPRORATESTEP6, 
                        src:RTPRORATESTEP7::numeric(38, 10) AS RTPRORATESTEP7, 
                        src:RTPRORATESTEP8::numeric(38, 10) AS RTPRORATESTEP8, 
                        src:RTPRORATESTEP9::numeric(38, 10) AS RTPRORATESTEP9, 
                        src:RTRATECODE::varchar AS RTRATECODE, 
                        src:RTSTDDAYS::integer AS RTSTDDAYS, 
                        src:RTUNIT::integer AS RTUNIT, 
                        src:RTUOM::varchar AS RTUOM, 
                        src:RTUSG1::numeric(38, 10) AS RTUSG1, 
                        src:RTUSG10::numeric(38, 10) AS RTUSG10, 
                        src:RTUSG2::numeric(38, 10) AS RTUSG2, 
                        src:RTUSG3::numeric(38, 10) AS RTUSG3, 
                        src:RTUSG4::numeric(38, 10) AS RTUSG4, 
                        src:RTUSG5::numeric(38, 10) AS RTUSG5, 
                        src:RTUSG6::numeric(38, 10) AS RTUSG6, 
                        src:RTUSG7::numeric(38, 10) AS RTUSG7, 
                        src:RTUSG8::numeric(38, 10) AS RTUSG8, 
                        src:RTUSG9::numeric(38, 10) AS RTUSG9, 
                        src:RTUSGDAYS::integer AS RTUSGDAYS, 
                        src:SINGLERATE::numeric(38, 10) AS SINGLERATE, 
                        src:SRVUSGDAYS::integer AS SRVUSGDAYS, 
                        src:TOTALBILLABLEUSAGE::numeric(38, 10) AS TOTALBILLABLEUSAGE, 
                        src:TOTALUSAGEDAYS::integer AS TOTALUSAGEDAYS, 
                        src:USEFORPRORATING::integer AS USEFORPRORATING, 
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
                                        
                src:CALCULATIONDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BASEBILLABLEUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CALCULATIONDETAILKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CALCULATIONTABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONVERTEDUNITS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAYSBETWEENREADS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FULLAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFSTEPS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRORATEAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RATETABLEDETAILKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTCHG9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTMAXCHG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTMINCHG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTPRORATESTEP9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTSTDDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUNIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSG9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RTUSGDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SINGLERATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SRVUSGDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALBILLABLEUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOTALUSAGEDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null