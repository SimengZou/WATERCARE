CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_PAYMENTREVERSALREASON AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALERTDAYS::integer AS ALERTDAYS, 
                        src:ALERTDEFINITIONKEY::integer AS ALERTDEFINITIONKEY, 
                        src:BACKCOLOR::integer AS BACKCOLOR, 
                        src:CODE::varchar AS CODE, 
                        src:CREDITRATINGPOINTSCODE::varchar AS CREDITRATINGPOINTSCODE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPT::varchar AS DESCRIPT, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FORTRANSFERPAYMENTFLAG::varchar AS FORTRANSFERPAYMENTFLAG, 
                        src:LOGTYPE::varchar AS LOGTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NSFDEFAULTFLAG::varchar AS NSFDEFAULTFLAG, 
                        src:NSFEXCEPTIONFLAG::varchar AS NSFEXCEPTIONFLAG, 
                        src:NSFTYPEFLAG::varchar AS NSFTYPEFLAG, 
                        src:OVERRIDENSFFEEFLAG::varchar AS OVERRIDENSFFEEFLAG, 
                        src:SENDNOTICEFLAG::varchar AS SENDNOTICEFLAG, 
                        src:TEXTCOLOR::integer AS TEXTCOLOR, 
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
    
                        
                src:CODE,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CODE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_PAYMENTREVERSALREASON as strm))