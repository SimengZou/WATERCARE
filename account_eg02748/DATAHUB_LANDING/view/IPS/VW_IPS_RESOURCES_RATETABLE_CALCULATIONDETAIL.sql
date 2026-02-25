CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:CALCULATIONDETAILKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CALCULATIONDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_RATETABLE_CALCULATIONDETAIL as strm))