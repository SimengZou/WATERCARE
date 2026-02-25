CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERLINEARPREFERENCES AS SELECT
                        src:ULP_CODE::varchar AS ULP_CODE, 
                        src:ULP_CREATED::datetime AS ULP_CREATED, 
                        src:ULP_CREATEDBY::varchar AS ULP_CREATEDBY, 
                        src:ULP_DEFAULT::varchar AS ULP_DEFAULT, 
                        src:ULP_DESC::varchar AS ULP_DESC, 
                        src:ULP_GROUPSEGMENTS::varchar AS ULP_GROUPSEGMENTS, 
                        src:ULP_HIDEROUES::varchar AS ULP_HIDEROUES, 
                        src:ULP_HIDESEGMENTS::varchar AS ULP_HIDESEGMENTS, 
                        src:ULP_LASTSAVED::datetime AS ULP_LASTSAVED, 
                        src:ULP_LODEFAULTCOLOR::varchar AS ULP_LODEFAULTCOLOR, 
                        src:ULP_LODEFAULTFILTER::varchar AS ULP_LODEFAULTFILTER, 
                        src:ULP_LOINCLUDERELATED::varchar AS ULP_LOINCLUDERELATED, 
                        src:ULP_LOLINEARREF::varchar AS ULP_LOLINEARREF, 
                        src:ULP_LOPOINTSOFINT::varchar AS ULP_LOPOINTSOFINT, 
                        src:ULP_LORELATEDEQ::varchar AS ULP_LORELATEDEQ, 
                        src:ULP_LORELATEDPART::varchar AS ULP_LORELATEDPART, 
                        src:ULP_LOROUTEANDSEGMENT::varchar AS ULP_LOROUTEANDSEGMENT, 
                        src:ULP_UPDATECOUNT::numeric(38, 10) AS ULP_UPDATECOUNT, 
                        src:ULP_UPDATED::datetime AS ULP_UPDATED, 
                        src:ULP_UPDATEDBY::varchar AS ULP_UPDATEDBY, 
                        src:ULP_USERCODE::varchar AS ULP_USERCODE, 
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
    
                        
                src:ULP_CODE,
            src:ULP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ULP_CODE  ORDER BY 
            src:ULP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERLINEARPREFERENCES as strm))