CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APDTTM::datetime AS APDTTM, 
                        src:APNO::varchar AS APNO, 
                        src:CUSTOMERNAME::varchar AS CUSTOMERNAME, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DUEDAYS::integer AS DUEDAYS, 
                        src:FEEPAYERNAME::varchar AS FEEPAYERNAME, 
                        src:FROMEMAIL::varchar AS FROMEMAIL, 
                        src:GST::numeric(38, 10) AS GST, 
                        src:KEYCOLUMN::integer AS KEYCOLUMN, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONIKER::varchar AS MONIKER, 
                        src:NAME::varchar AS NAME, 
                        src:OCCUPANCYTYPE::varchar AS OCCUPANCYTYPE, 
                        src:QUOTEACCEPTANCEEMAIL::varchar AS QUOTEACCEPTANCEEMAIL, 
                        src:QUOTEGENERATIONSTAGINGKEY::integer AS QUOTEGENERATIONSTAGINGKEY, 
                        src:QUOTETYPE::varchar AS QUOTETYPE, 
                        src:REVIEWERDDI::varchar AS REVIEWERDDI, 
                        src:REVIEWEREMAIL::varchar AS REVIEWEREMAIL, 
                        src:REVIEWERNAME::varchar AS REVIEWERNAME, 
                        src:REVIEWERROLE::varchar AS REVIEWERROLE, 
                        src:SOURCE::varchar AS SOURCE, 
                        src:STREETNUMBER::varchar AS STREETNUMBER, 
                        src:SUBJECT::varchar AS SUBJECT, 
                        src:TITLE::varchar AS TITLE, 
                        src:TOEMAIL::varchar AS TOEMAIL, 
                        src:TOTALAFTERTAX::numeric(38, 10) AS TOTALAFTERTAX, 
                        src:TOTALVALUE::numeric(38, 10) AS TOTALVALUE, 
                        src:VALIDTILL::datetime AS VALIDTILL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERCAREREFNO::varchar AS WATERCAREREFNO, 
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
    
                        
                src:QUOTEGENERATIONSTAGINGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:QUOTEGENERATIONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING as strm))