CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_SERVICEOPTIONS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSETTYPE::integer AS ASSETTYPE, 
                        src:AVAILABILITY::varchar AS AVAILABILITY, 
                        src:BUILDERFLAG::varchar AS BUILDERFLAG, 
                        src:CLASS::varchar AS CLASS, 
                        src:CODE::varchar AS CODE, 
                        src:CONSGRAPHFLAG::varchar AS CONSGRAPHFLAG, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DONOTPRORATEMOVEINFLAG::varchar AS DONOTPRORATEMOVEINFLAG, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FIXEDCHGINADVANCEFLAG::varchar AS FIXEDCHGINADVANCEFLAG, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:IMPERVIOUSAREAONLYFLAG::varchar AS IMPERVIOUSAREAONLYFLAG, 
                        src:LEARNMOREKBKEY::integer AS LEARNMOREKBKEY, 
                        src:METERTYPEFORMULA::integer AS METERTYPEFORMULA, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MOVEINACTKEY::integer AS MOVEINACTKEY, 
                        src:MOVEINCOLOR::integer AS MOVEINCOLOR, 
                        src:MOVEINNOTES::varchar AS MOVEINNOTES, 
                        src:MOVEINREQSKEY::integer AS MOVEINREQSKEY, 
                        src:MOVEINSRWOCONDITIONFORMULA::integer AS MOVEINSRWOCONDITIONFORMULA, 
                        src:MOVEINSWOKEY::integer AS MOVEINSWOKEY, 
                        src:MOVEOUTACTKEY::integer AS MOVEOUTACTKEY, 
                        src:MOVEOUTLEARNMOREKBKEY::integer AS MOVEOUTLEARNMOREKBKEY, 
                        src:MOVEOUTNOTES::varchar AS MOVEOUTNOTES, 
                        src:MOVEOUTREQSKEY::integer AS MOVEOUTREQSKEY, 
                        src:MOVEOUTSRWOCONDITIONFORMULA::integer AS MOVEOUTSRWOCONDITIONFORMULA, 
                        src:MOVEOUTSWOKEY::integer AS MOVEOUTSWOKEY, 
                        src:OUTPUTHISTORICALUSAGEFLAG::varchar AS OUTPUTHISTORICALUSAGEFLAG, 
                        src:OWNERFLAG::varchar AS OWNERFLAG, 
                        src:PRORATESERVICESTARTFLAG::varchar AS PRORATESERVICESTARTFLAG, 
                        src:PRORATESERVICESTOPFLAG::varchar AS PRORATESERVICESTOPFLAG, 
                        src:SERVICECLASS1::varchar AS SERVICECLASS1, 
                        src:SERVICECLASS2::varchar AS SERVICECLASS2, 
                        src:SERVICEDEFINITIONKEY::integer AS SERVICEDEFINITIONKEY, 
                        src:SERVICEDISTRICT::varchar AS SERVICEDISTRICT, 
                        src:SERVICEFIELD1KEY::integer AS SERVICEFIELD1KEY, 
                        src:SERVICEFIELD2KEY::integer AS SERVICEFIELD2KEY, 
                        src:SERVICEFIELD3KEY::integer AS SERVICEFIELD3KEY, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:SERVICETYPE::varchar AS SERVICETYPE, 
                        src:TENANTFLAG::varchar AS TENANTFLAG, 
                        src:TRADEWASTEFLAG::varchar AS TRADEWASTEFLAG, 
                        src:TRADEWASTEMAX::integer AS TRADEWASTEMAX, 
                        src:TRADEWASTERATE::integer AS TRADEWASTERATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WINTERAVGFLAG::varchar AS WINTERAVGFLAG, 
                        src:WINTERAVGSETUPKEY::integer AS WINTERAVGSETUPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:SERVICEOPTIONSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_SERVICEOPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LEARNMOREKBKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:METERTYPEFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEINACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEINCOLOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEINREQSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEINSRWOCONDITIONFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEINSWOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTLEARNMOREKBKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTREQSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTSRWOCONDITIONFORMULA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MOVEOUTSWOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEDEFINITIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD1KEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD2KEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEFIELD3KEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRADEWASTEMAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRADEWASTERATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WINTERAVGSETUPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null