CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DELINQUENCYMILESTONE AS SELECT
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
                        src:WRITEOFFELIGIBLEFLAG::varchar AS WRITEOFFELIGIBLEFLAG, 
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
    
                        
                src:MILESTONEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MILESTONEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DELINQUENCYMILESTONE as strm))