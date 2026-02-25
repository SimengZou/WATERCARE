
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5JOBPARAM
                   as                       
                    SELECT
                        t.JPR_NAME,
                        t.JPR_LINE,
                        t.JPR_PARAMETER,
                        t.JPR_RENTITY,
                        t.JPR_RTYPE,
                        t.JPR_DATATYPE,
                        t.JPR_LENGTH,
                        t.JPR_FORMAT,
                        t.JPR_DEFAULT,
                        t.JPR_FIXED,
                        t.JPR_MANDATORY,
                        t.JPR_UPPER,
                        t.JPR_OPTIONS,
                        t.JPR_REMEMBER,
                        t.JPR_TEST,
                        t.JPR_QUERY,
                        t.JPR_LOVFUNCTION,
                        t.JPR_PROPERTY,
                        t.JPR_EWSLOVDEF,
                        t.JPR_UPDATECOUNT,
                        t.JPR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5JOBPARAM as  t
					 union
					        SELECT
                        th.JPR_NAME,
                        th.JPR_LINE,
                        th.JPR_PARAMETER,
                        th.JPR_RENTITY,
                        th.JPR_RTYPE,
                        th.JPR_DATATYPE,
                        th.JPR_LENGTH,
                        th.JPR_FORMAT,
                        th.JPR_DEFAULT,
                        th.JPR_FIXED,
                        th.JPR_MANDATORY,
                        th.JPR_UPPER,
                        th.JPR_OPTIONS,
                        th.JPR_REMEMBER,
                        th.JPR_TEST,
                        th.JPR_QUERY,
                        th.JPR_LOVFUNCTION,
                        th.JPR_PROPERTY,
                        th.JPR_EWSLOVDEF,
                        th.JPR_UPDATECOUNT,
                        th.JPR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5JOBPARAM as  th ;
                     