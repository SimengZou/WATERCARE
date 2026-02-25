CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_U5SERVICEREQUEST AS SELECT
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:CRM_ADDRESSKEY::varchar AS CRM_ADDRESSKEY, 
                        src:CRM_ALTERNATECONTACT::varchar AS CRM_ALTERNATECONTACT, 
                        src:CRM_CALLERFISTNAME::varchar AS CRM_CALLERFISTNAME, 
                        src:CRM_CALLERLASTNAME::varchar AS CRM_CALLERLASTNAME, 
                        src:CRM_CALLTAKENBY::varchar AS CRM_CALLTAKENBY, 
                        src:CRM_CITY::varchar AS CRM_CITY, 
                        src:CRM_CONTACTEMAIL::varchar AS CRM_CONTACTEMAIL, 
                        src:CRM_CONTACTNUMBER::varchar AS CRM_CONTACTNUMBER, 
                        src:CRM_CONTACTREQUESTED::varchar AS CRM_CONTACTREQUESTED, 
                        src:CRM_EVENT::varchar AS CRM_EVENT, 
                        src:CRM_FLATNO::varchar AS CRM_FLATNO, 
                        src:CRM_NOTES::varchar AS CRM_NOTES, 
                        src:CRM_POSTALCODE::varchar AS CRM_POSTALCODE, 
                        src:CRM_PRIORITY::varchar AS CRM_PRIORITY, 
                        src:CRM_PROBLEMCODE::varchar AS CRM_PROBLEMCODE, 
                        src:CRM_REPORTEDDATE::datetime AS CRM_REPORTEDDATE, 
                        src:CRM_SERVICEREQUEST::varchar AS CRM_SERVICEREQUEST, 
                        src:CRM_SOURCE::varchar AS CRM_SOURCE, 
                        src:CRM_STNAME::varchar AS CRM_STNAME, 
                        src:CRM_STREETNO::varchar AS CRM_STREETNO, 
                        src:CRM_SUBURB::varchar AS CRM_SUBURB, 
                        src:CRM_TRANSACTIONID::varchar AS CRM_TRANSACTIONID, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:CRM_TRANSACTIONID,
            src:LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CRM_TRANSACTIONID  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_U5SERVICEREQUEST as strm))