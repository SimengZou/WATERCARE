CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_APTRN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJREAS::varchar AS ADJREAS, 
                        src:APFEEKEY::integer AS APFEEKEY, 
                        src:APKEY::integer AS APKEY, 
                        src:APPAYKEY::integer AS APPAYKEY, 
                        src:APRFNDTRNNO::integer AS APRFNDTRNNO, 
                        src:APTRNNO::integer AS APTRNNO, 
                        src:AUTH::varchar AS AUTH, 
                        src:BGTNOKEY::integer AS BGTNOKEY, 
                        src:CDRPRODFAMILY::integer AS CDRPRODFAMILY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:DELETED::boolean AS DELETED, 
                        src:DSCFEEKEY::integer AS DSCFEEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEEDSJOURNAL::varchar AS NEEDSJOURNAL, 
                        src:REFTRNNO::integer AS REFTRNNO, 
                        src:TRNAMT::numeric(38, 10) AS TRNAMT, 
                        src:TRNBY::varchar AS TRNBY, 
                        src:TRNDTTM::datetime AS TRNDTTM, 
                        src:TRNTYPE::varchar AS TRNTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XFRAPKEY::integer AS XFRAPKEY, 
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
    
                        
                src:APTRNNO,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APTRNNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_APTRN as strm))