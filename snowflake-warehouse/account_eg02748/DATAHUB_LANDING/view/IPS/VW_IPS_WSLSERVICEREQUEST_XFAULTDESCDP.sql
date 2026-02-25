CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLSERVICEREQUEST_XFAULTDESCDP AS SELECT
                        src:ACCESSISSUES::varchar AS ACCESSISSUES, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSETID::varchar AS ASSETID, 
                        src:BOTTLEDWATER::varchar AS BOTTLEDWATER, 
                        src:CCTV::varchar AS CCTV, 
                        src:CONTRACTORAREA::varchar AS CONTRACTORAREA, 
                        src:CONTRACTORCODE::varchar AS CONTRACTORCODE, 
                        src:CONTRACTORREFNO::varchar AS CONTRACTORREFNO, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIALYSISPATIENT::varchar AS DIALYSISPATIENT, 
                        src:DOCTORCALLED::varchar AS DOCTORCALLED, 
                        src:FAULTLOCATION::varchar AS FAULTLOCATION, 
                        src:FAULTTYPE::varchar AS FAULTTYPE, 
                        src:FLUSH::varchar AS FLUSH, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:GATEVALVE::varchar AS GATEVALVE, 
                        src:HEALTHSAFETY::varchar AS HEALTHSAFETY, 
                        src:INTERNALDRAINAGE::varchar AS INTERNALDRAINAGE, 
                        src:JOBCBD::varchar AS JOBCBD, 
                        src:KEYACCOUNT::varchar AS KEYACCOUNT, 
                        src:METERASSETID::varchar AS METERASSETID, 
                        src:METERASSETLOCATION::varchar AS METERASSETLOCATION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIPLEPROPERTIES::varchar AS MULTIPLEPROPERTIES, 
                        src:NEIGHBOURSAFFECTED::varchar AS NEIGHBOURSAFFECTED, 
                        src:NUMBERSRLAST::varchar AS NUMBERSRLAST, 
                        src:PHOTOSPROVIDED::varchar AS PHOTOSPROVIDED, 
                        src:PLANNEDMETER::varchar AS PLANNEDMETER, 
                        src:PLUMBERCONTACTED::varchar AS PLUMBERCONTACTED, 
                        src:PLUMBINGWORK::varchar AS PLUMBINGWORK, 
                        src:PROJECTRELATED::varchar AS PROJECTRELATED, 
                        src:PROPERTYDAMAGE::varchar AS PROPERTYDAMAGE, 
                        src:PUBLICHEALTHRISK::varchar AS PUBLICHEALTHRISK, 
                        src:RAINWATERTANK::varchar AS RAINWATERTANK, 
                        src:RELATEDSR::varchar AS RELATEDSR, 
                        src:RESIDENTFEELING::datetime AS RESIDENTFEELING, 
                        src:RESIDENTILL::varchar AS RESIDENTILL, 
                        src:ROADWATERMAINBREAK::varchar AS ROADWATERMAINBREAK, 
                        src:SCHEDULEDSITETREE::varchar AS SCHEDULEDSITETREE, 
                        src:SERVICEAREA::varchar AS SERVICEAREA, 
                        src:SERVNO::integer AS SERVNO, 
                        src:SIZEOFLEAK::varchar AS SIZEOFLEAK, 
                        src:SUPERVISOR::varchar AS SUPERVISOR, 
                        src:SYMPTOMSSTARTED::datetime AS SYMPTOMSSTARTED, 
                        src:TAPS::varchar AS TAPS, 
                        src:THIRDPARTY::varchar AS THIRDPARTY, 
                        src:TREE::varchar AS TREE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WTRCOLORSMELLPART::varchar AS WTRCOLORSMELLPART, 
                        src:XFAULTDESCDPKEY::integer AS XFAULTDESCDPKEY, 
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
    
                        
                src:XFAULTDESCDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XFAULTDESCDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLSERVICEREQUEST_XFAULTDESCDP as strm))