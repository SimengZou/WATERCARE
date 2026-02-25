CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AREAHECTARES::numeric(38, 10) AS AREAHECTARES, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COMPLETEMONTH::integer AS COMPLETEMONTH, 
                        src:COMPLETEYEAR1::integer AS COMPLETEYEAR1, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOOFUNITS::integer AS NOOFUNITS, 
                        src:STAGE::varchar AS STAGE, 
                        src:STARTMONTH::integer AS STARTMONTH, 
                        src:STARTYEAR1::integer AS STARTYEAR1, 
                        src:STATUS::varchar AS STATUS, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPLANAPTIMELINEDPKEY::integer AS XPLANAPTIMELINEDPKEY, 
                        src:XPLANAPTIMELINEGDKEY::integer AS XPLANAPTIMELINEGDKEY, 
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
    
                        
                src:XPLANAPTIMELINEGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPLANAPTIMELINEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPLAN_XPLANAPTIMELINEGD as strm))