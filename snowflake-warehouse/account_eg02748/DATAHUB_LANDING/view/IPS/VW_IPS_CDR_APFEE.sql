CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_APFEE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMT::numeric(38, 10) AS AMT, 
                        src:APFEEKEY::integer AS APFEEKEY, 
                        src:APFEETYPEKEY::integer AS APFEETYPEKEY, 
                        src:APKEY::integer AS APKEY, 
                        src:APPLDBY::varchar AS APPLDBY, 
                        src:APPLDDTTM::datetime AS APPLDDTTM, 
                        src:BGTNOKEY::integer AS BGTNOKEY, 
                        src:BILLACCTKEY::integer AS BILLACCTKEY, 
                        src:BILLNO::integer AS BILLNO, 
                        src:CDRPRODFAMILY::integer AS CDRPRODFAMILY, 
                        src:CMPTRGEN::varchar AS CMPTRGEN, 
                        src:DELETED::boolean AS DELETED, 
                        src:FEECODE::varchar AS FEECODE, 
                        src:FEEDESC::varchar AS FEEDESC, 
                        src:FEETYPEVERSIONKEY::integer AS FEETYPEVERSIONKEY, 
                        src:HIDEINAPPL::varchar AS HIDEINAPPL, 
                        src:INITDEPFEEKEY::integer AS INITDEPFEEKEY, 
                        src:LIENED::varchar AS LIENED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAIDDTTM::datetime AS PAIDDTTM, 
                        src:PAYORD::integer AS PAYORD, 
                        src:PENRUNKEY::integer AS PENRUNKEY, 
                        src:PNAPFEEKEY::integer AS PNAPFEEKEY, 
                        src:PNRVRUNKEY::integer AS PNRVRUNKEY, 
                        src:QTY::numeric(38, 10) AS QTY, 
                        src:REFUNDABLE::varchar AS REFUNDABLE, 
                        src:SRCBGTNOKEY::integer AS SRCBGTNOKEY, 
                        src:STAT::varchar AS STAT, 
                        src:VAL::numeric(38, 10) AS VAL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WAIVABLE::varchar AS WAIVABLE, 
                        src:WAIVED::varchar AS WAIVED, 
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
    
                        
                src:APFEEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APFEEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_APFEE as strm))