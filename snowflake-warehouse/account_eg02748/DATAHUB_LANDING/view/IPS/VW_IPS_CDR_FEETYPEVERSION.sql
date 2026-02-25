CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_FEETYPEVERSION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:FEETYPEKEY::integer AS FEETYPEKEY, 
                        src:FEETYPEVERSIONKEY::integer AS FEETYPEVERSIONKEY, 
                        src:MAXFEEAMT::numeric(38, 10) AS MAXFEEAMT, 
                        src:MINBALANCE::numeric(38, 10) AS MINBALANCE, 
                        src:MINBALANCECALCTYPE::integer AS MINBALANCECALCTYPE, 
                        src:MINBALANCEFRMLA::integer AS MINBALANCEFRMLA, 
                        src:MINFEEAMT::numeric(38, 10) AS MINFEEAMT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:QTY::numeric(38, 10) AS QTY, 
                        src:QTYFRMLA::integer AS QTYFRMLA, 
                        src:QUANTITYCALCTYPE::integer AS QUANTITYCALCTYPE, 
                        src:USEMAXFEEAMT::varchar AS USEMAXFEEAMT, 
                        src:USEMINFEEAMT::varchar AS USEMINFEEAMT, 
                        src:VALUE::numeric(38, 10) AS VALUE, 
                        src:VALUECALCTYPE::integer AS VALUECALCTYPE, 
                        src:VALUEFRMLA::integer AS VALUEFRMLA, 
                        src:VALUERATECODEKEY::integer AS VALUERATECODEKEY, 
                        src:VALUERATETBLFRMLAKEY::integer AS VALUERATETBLFRMLAKEY, 
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
    
                        
                src:FEETYPEVERSIONKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FEETYPEVERSIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_FEETYPEVERSION as strm))