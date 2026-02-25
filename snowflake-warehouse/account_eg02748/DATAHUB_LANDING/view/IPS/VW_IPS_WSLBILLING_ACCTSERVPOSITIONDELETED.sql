CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_ACCTSERVPOSITIONDELETED AS SELECT
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ACCOUNTSERVICEPOSITIONKEY::integer AS ACCOUNTSERVICEPOSITIONKEY, 
                        src:ACCTSERVPOSITIONDELETEDKEY::integer AS ACCTSERVPOSITIONDELETEDKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ASSETTYPE::integer AS ASSETTYPE, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:GRAPHUSAGEFLAG::varchar AS GRAPHUSAGEFLAG, 
                        src:HIDEREADINGSONBILL::varchar AS HIDEREADINGSONBILL, 
                        src:METERREGISTERUSE::integer AS METERREGISTERUSE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MOVEINREADINGKEY::integer AS MOVEINREADINGKEY, 
                        src:MOVEOUTREADINGKEY::integer AS MOVEOUTREADINGKEY, 
                        src:ORIGADDBY::varchar AS ORIGADDBY, 
                        src:ORIGADDDTTM::datetime AS ORIGADDDTTM, 
                        src:ORIGMODBY::varchar AS ORIGMODBY, 
                        src:ORIGMODDTTM::datetime AS ORIGMODDTTM, 
                        src:POSITION::integer AS POSITION, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
                        src:SUBTRACTIVEFLAG::varchar AS SUBTRACTIVEFLAG, 
                        src:USEDAYSRDS::varchar AS USEDAYSRDS, 
                        src:USEUOM::varchar AS USEUOM, 
                        src:USGHISTINBILLOUTPUT::varchar AS USGHISTINBILLOUTPUT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WINTERAVG::numeric(38, 10) AS WINTERAVG, 
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
    
                        
                src:ACCTSERVPOSITIONDELETEDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCTSERVPOSITIONDELETEDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_ACCTSERVPOSITIONDELETED as strm))