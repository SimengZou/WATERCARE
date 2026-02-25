CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOWARCHIVEFLAG::varchar AS ALLOWARCHIVEFLAG, 
                        src:ALLOWOUTSTANDINGROUTESFLAG::varchar AS ALLOWOUTSTANDINGROUTESFLAG, 
                        src:CONCATENATEFILEFLAG::varchar AS CONCATENATEFILEFLAG, 
                        src:DAYSBEFORENEXTREADINGDAY::integer AS DAYSBEFORENEXTREADINGDAY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELETEPREVFILEVERFLAG::varchar AS DELETEPREVFILEVERFLAG, 
                        src:EXCLBILLEDDAYS::integer AS EXCLBILLEDDAYS, 
                        src:EXCLBILLEDFLAG::varchar AS EXCLBILLEDFLAG, 
                        src:EXCLEXCEPMETERS::varchar AS EXCLEXCEPMETERS, 
                        src:EXCLLASTREADDAYS::integer AS EXCLLASTREADDAYS, 
                        src:EXCLLASTREADFLAG::varchar AS EXCLLASTREADFLAG, 
                        src:EXCLNEWMETERS::varchar AS EXCLNEWMETERS, 
                        src:EXCLNOACCTMETERS::varchar AS EXCLNOACCTMETERS, 
                        src:EXCLSRAFTERDAYS::integer AS EXCLSRAFTERDAYS, 
                        src:EXCLSRBEFOREDAYS::integer AS EXCLSRBEFOREDAYS, 
                        src:EXCLSRFLAG::varchar AS EXCLSRFLAG, 
                        src:EXCLZEROREADINGMETERS::varchar AS EXCLZEROREADINGMETERS, 
                        src:EXCLZEROUSAGEMETERS::varchar AS EXCLZEROUSAGEMETERS, 
                        src:EXPORTDIRPATH::varchar AS EXPORTDIRPATH, 
                        src:EXPORTRUNNUMBER::integer AS EXPORTRUNNUMBER, 
                        src:EXPORTTEMPLATEFILE::varchar AS EXPORTTEMPLATEFILE, 
                        src:EXPORTTYPE::integer AS EXPORTTYPE, 
                        src:FILENUMBERCOUNTER::integer AS FILENUMBERCOUNTER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:READINGEXPORTSETUPKEY::integer AS READINGEXPORTSETUPKEY, 
                        src:RECEIVERCODE::varchar AS RECEIVERCODE, 
                        src:SENDERCODE::varchar AS SENDERCODE, 
                        src:SORTBYCOLUMNS::varchar AS SORTBYCOLUMNS, 
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
    
                        
                src:READINGEXPORTSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGEXPORTSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP as strm))