CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WORKMANAGEMENT_COSTXTRA AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHRGDTTM::datetime AS CHRGDTTM, 
                        src:CHRGDTTMTO::datetime AS CHRGDTTMTO, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COSTKEY::integer AS COSTKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DSTBGTKEY::integer AS DSTBGTKEY, 
                        src:DSTBGTNO::varchar AS DSTBGTNO, 
                        src:EXTRAITEM::varchar AS EXTRAITEM, 
                        src:HISTKEY::integer AS HISTKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RATE::numeric(38, 10) AS RATE, 
                        src:SRC::varchar AS SRC, 
                        src:SRCBGTKEY::integer AS SRCBGTKEY, 
                        src:SRCBGTNO::varchar AS SRCBGTNO, 
                        src:TASKKEY::integer AS TASKKEY, 
                        src:TOTCOST::numeric(38, 10) AS TOTCOST, 
                        src:USAGE::numeric(38, 10) AS USAGE, 
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
    
                        
                src:COSTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:COSTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WORKMANAGEMENT_COSTXTRA as strm))