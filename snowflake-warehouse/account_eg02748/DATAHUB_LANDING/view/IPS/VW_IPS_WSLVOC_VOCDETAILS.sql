CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLVOC_VOCDETAILS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SERVNO::integer AS SERVNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VCA::varchar AS VCA, 
                        src:VCH::varchar AS VCH, 
                        src:VCO::varchar AS VCO, 
                        src:VFD::varchar AS VFD, 
                        src:VFO::varchar AS VFO, 
                        src:VFR::varchar AS VFR, 
                        src:VOCDETAILSKEY::integer AS VOCDETAILSKEY, 
                        src:VON::varchar AS VON, 
                        src:VPA::varchar AS VPA, 
                        src:VPB::varchar AS VPB, 
                        src:VPC::varchar AS VPC, 
                        src:VPI::varchar AS VPI, 
                        src:VPL::varchar AS VPL, 
                        src:VPM::varchar AS VPM, 
                        src:VPO::varchar AS VPO, 
                        src:VPP::varchar AS VPP, 
                        src:VPT::varchar AS VPT, 
                        src:VSA::varchar AS VSA, 
                        src:VSE::varchar AS VSE, 
                        src:VSH::varchar AS VSH, 
                        src:VSI::varchar AS VSI, 
                        src:VSK::varchar AS VSK, 
                        src:VSL::varchar AS VSL, 
                        src:VSO::varchar AS VSO, 
                        src:VSQ::varchar AS VSQ, 
                        src:VSR::varchar AS VSR, 
                        src:VWA::varchar AS VWA, 
                        src:VWF::varchar AS VWF, 
                        src:VWN::varchar AS VWN, 
                        src:VWO::varchar AS VWO, 
                        src:VWP::varchar AS VWP, 
                        src:VWR::varchar AS VWR, 
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
    
                        
                src:VOCDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:VOCDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLVOC_VOCDETAILS as strm))