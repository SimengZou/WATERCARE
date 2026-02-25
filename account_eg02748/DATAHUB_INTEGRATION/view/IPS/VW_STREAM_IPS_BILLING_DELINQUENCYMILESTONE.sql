CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYMILESTONE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTODECREMENTBELOWAMT::numeric(38, 10) AS AUTODECREMENTBELOWAMT, 
                        src:AUTODECREMENTMILESTONEKEY::integer AS AUTODECREMENTMILESTONEKEY, 
                        src:AUTODECREMENTMSFORMULA::integer AS AUTODECREMENTMSFORMULA, 
                        src:AUTODECREMENTMSSPECIFIER::varchar AS AUTODECREMENTMSSPECIFIER, 
                        src:CODE::varchar AS CODE, 
                        src:COLLECTIONSELIGIBLEFLAG::varchar AS COLLECTIONSELIGIBLEFLAG, 
                        src:COLLECTIONSRESOLUTIONCODE::varchar AS COLLECTIONSRESOLUTIONCODE, 
                        src:COLLRESCODEFORMULAKEY::integer AS COLLRESCODEFORMULAKEY, 
                        src:COLLRESCODESPECIFIER::varchar AS COLLRESCODESPECIFIER, 
                        src:CREDITRATINGAPPLYTO::varchar AS CREDITRATINGAPPLYTO, 
                        src:CREDITRATINGPOINTS::integer AS CREDITRATINGPOINTS, 
                        src:CREDITRATINGPOINTSFORMULA::integer AS CREDITRATINGPOINTSFORMULA, 
                        src:CREDITRATINGSPECIFIER::varchar AS CREDITRATINGSPECIFIER, 
                        src:DECREMENTMILESTONEFORMULA::integer AS DECREMENTMILESTONEFORMULA, 
                        src:DECREMENTMILESTONKEY::integer AS DECREMENTMILESTONKEY, 
                        src:DECREMENTMILESTONSPECIFIER::varchar AS DECREMENTMILESTONSPECIFIER, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELINQUENCYLEVEL::varchar AS DELINQUENCYLEVEL, 
                        src:DISCONNECTMILESTONE::varchar AS DISCONNECTMILESTONE, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:DUEAFTER::integer AS DUEAFTER, 
                        src:DUEAFTERDAYTYPE::varchar AS DUEAFTERDAYTYPE, 
                        src:DUEDATEENDONVALIDDAYTYPE::varchar AS DUEDATEENDONVALIDDAYTYPE, 
                        src:DUEDATESEARCHDIRECTION::varchar AS DUEDATESEARCHDIRECTION, 
                        src:DURATION::integer AS DURATION, 
                        src:DURATIONDAYTYPE::varchar AS DURATIONDAYTYPE, 
                        src:DURATIONFORMULA::integer AS DURATIONFORMULA, 
                        src:DURATIONSPECIFIER::varchar AS DURATIONSPECIFIER, 
                        src:LIENELIGIBLEFLAG::varchar AS LIENELIGIBLEFLAG, 
                        src:LIENRESCODEFORMULAKEY::integer AS LIENRESCODEFORMULAKEY, 
                        src:LIENRESCODESPECIFIER::varchar AS LIENRESCODESPECIFIER, 
                        src:LIENRESOLUTIONCODE::varchar AS LIENRESOLUTIONCODE, 
                        src:MILESTONEKEY::integer AS MILESTONEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTMILESTONEFORMULA::integer AS NEXTMILESTONEFORMULA, 
                        src:NEXTMILESTONEKEY::integer AS NEXTMILESTONEKEY, 
                        src:NEXTMILESTONESPECIFIER::varchar AS NEXTMILESTONESPECIFIER, 
                        src:RESOLUTIONCODE::varchar AS RESOLUTIONCODE, 
                        src:RESOLUTIONCODEFORMULA::integer AS RESOLUTIONCODEFORMULA, 
                        src:RESOLUTIONCODESPECIFIER::varchar AS RESOLUTIONCODESPECIFIER, 
                        src:RESOLVEBELOWAMT::numeric(38, 10) AS RESOLVEBELOWAMT, 
                        src:RESOLVEFORMULA::integer AS RESOLVEFORMULA, 
                        src:RESOLVESPECIFIER::varchar AS RESOLVESPECIFIER, 
                        src:REVERSEPAYMILESTONEFORMULA::integer AS REVERSEPAYMILESTONEFORMULA, 
                        src:REVERSEPAYMILESTONEKEY::integer AS REVERSEPAYMILESTONEKEY, 
                        src:REVERSEPAYMSSPECIFIER::varchar AS REVERSEPAYMSSPECIFIER, 
                        src:SCHEMEKEY::integer AS SCHEMEKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WRITEOFFELIGIBLEFLAG::varchar AS WRITEOFFELIGIBLEFLAG, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:MILESTONEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DELINQUENCYMILESTONE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTODECREMENTBELOWAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTODECREMENTMILESTONEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTODECREMENTMSFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COLLRESCODEFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CREDITRATINGPOINTS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CREDITRATINGPOINTSFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DECREMENTMILESTONEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DECREMENTMILESTONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DUEAFTER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DURATIONFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LIENRESCODEFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MILESTONEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEXTMILESTONEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEXTMILESTONEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESOLUTIONCODEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESOLVEBELOWAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESOLVEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEPAYMILESTONEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEPAYMILESTONEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEMEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null