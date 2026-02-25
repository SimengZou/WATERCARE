CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_APSUSYS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTOCON::varchar AS AUTOCON, 
                        src:BLDGIRAUTOASSIGNFAILEDLOGTYPE::varchar AS BLDGIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:CASEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS CASEIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:CASHBGTNOKEY::integer AS CASHBGTNOKEY, 
                        src:CDRPRODFAMILY::integer AS CDRPRODFAMILY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:LICIRAUTOASSIGNFAILEDLOGTYPE::varchar AS LICIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OVERBGTNOKEY::integer AS OVERBGTNOKEY, 
                        src:PLANIRAUTOASSIGNFAILEDLOGTYPE::varchar AS PLANIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:PROJIRAUTOASSIGNFAILEDLOGTYPE::varchar AS PROJIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:PROPERTYOPTION::integer AS PROPERTYOPTION, 
                        src:REVBGTNOKEY::integer AS REVBGTNOKEY, 
                        src:RNDBGTNOKEY::integer AS RNDBGTNOKEY, 
                        src:SHOWASSET::varchar AS SHOWASSET, 
                        src:STARTTAB::integer AS STARTTAB, 
                        src:TRADEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS TRADEIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:USEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS USEIRAUTOASSIGNFAILEDLOGTYPE, 
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
    
                        
                src:CDRPRODFAMILY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CDRPRODFAMILY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_APSUSYS as strm))