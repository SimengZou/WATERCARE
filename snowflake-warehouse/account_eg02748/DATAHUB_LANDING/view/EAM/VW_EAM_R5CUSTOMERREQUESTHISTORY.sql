CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CUSTOMERREQUESTHISTORY AS SELECT
                        src:CRH_CALLCENTERCODE::varchar AS CRH_CALLCENTERCODE, 
                        src:CRH_CALLCENTER_ORG::varchar AS CRH_CALLCENTER_ORG, 
                        src:CRH_COMMENT::varchar AS CRH_COMMENT, 
                        src:CRH_EVENT::varchar AS CRH_EVENT, 
                        src:CRH_EVENTTYPE::varchar AS CRH_EVENTTYPE, 
                        src:CRH_FIELD::varchar AS CRH_FIELD, 
                        src:CRH_LASTSAVED::datetime AS CRH_LASTSAVED, 
                        src:CRH_NEWVALUE::varchar AS CRH_NEWVALUE, 
                        src:CRH_OLDVALUE::varchar AS CRH_OLDVALUE, 
                        src:CRH_PK::varchar AS CRH_PK, 
                        src:CRH_RENTITY::varchar AS CRH_RENTITY, 
                        src:CRH_REQDATE::datetime AS CRH_REQDATE, 
                        src:CRH_UPDATECOUNT::numeric(38, 10) AS CRH_UPDATECOUNT, 
                        src:CRH_USERCODE::varchar AS CRH_USERCODE, 
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
    
                        
                src:CRH_PK,
            src:CRH_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CRH_PK  ORDER BY 
            src:CRH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CUSTOMERREQUESTHISTORY as strm))