CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_READINGIMPORTSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:FORCEEXPORTSELECTIONFLAG::varchar AS FORCEEXPORTSELECTIONFLAG, 
                        src:IMPEXPDAYS::integer AS IMPEXPDAYS, 
                        src:IMPORTDIRPATH::varchar AS IMPORTDIRPATH, 
                        src:IMPORTFILETEMPLATE::varchar AS IMPORTFILETEMPLATE, 
                        src:IMPORTRUNNUMBER::integer AS IMPORTRUNNUMBER, 
                        src:IMPORTTYPE::integer AS IMPORTTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROCESSREADYTOBILLFLAG::varchar AS PROCESSREADYTOBILLFLAG, 
                        src:READINGIMPORTSETUPKEY::integer AS READINGIMPORTSETUPKEY, 
                        src:RECEIVERCODE::varchar AS RECEIVERCODE, 
                        src:REJECTNONEXROUTE::varchar AS REJECTNONEXROUTE, 
                        src:REJECTPRIORMETERREAD::varchar AS REJECTPRIORMETERREAD, 
                        src:RESOLVESRFLAG::varchar AS RESOLVESRFLAG, 
                        src:SENDERCODE::varchar AS SENDERCODE, 
                        src:SHOWFILEOPENDIALOGFLAG::varchar AS SHOWFILEOPENDIALOGFLAG, 
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
    
                        
                src:READINGIMPORTSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGIMPORTSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_READINGIMPORTSETUP as strm))