
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPPARMS
                   as                       
                    SELECT
                        t.PMT_FUNCTION,
                        t.PMT_LINE,
                        t.PMT_PARAMETER,
                        t.PMT_RENTITY,
                        t.PMT_RTYPE,
                        t.PMT_DATATYPE,
                        t.PMT_LENGTH,
                        t.PMT_FORMAT,
                        t.PMT_DEFAULT,
                        t.PMT_FIXED,
                        t.PMT_MANDATORY,
                        t.PMT_UPPER,
                        t.PMT_OPTIONS,
                        t.PMT_REMEMBER,
                        t.PMT_TEST,
                        t.PMT_QUERY,
                        t.PMT_LOVFUNCTION,
                        t.PMT_PROPERTY,
                        t.PMT_UPDATECOUNT,
                        t.PMT_EWSLOVDEF,
                        t.PMT_BOTTEXT,
                        t.PMT_UPDATED,
                        t.PMT_MEKEY,
                        t.PMT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPPARMS as  t
					 union
					        SELECT
                        th.PMT_FUNCTION,
                        th.PMT_LINE,
                        th.PMT_PARAMETER,
                        th.PMT_RENTITY,
                        th.PMT_RTYPE,
                        th.PMT_DATATYPE,
                        th.PMT_LENGTH,
                        th.PMT_FORMAT,
                        th.PMT_DEFAULT,
                        th.PMT_FIXED,
                        th.PMT_MANDATORY,
                        th.PMT_UPPER,
                        th.PMT_OPTIONS,
                        th.PMT_REMEMBER,
                        th.PMT_TEST,
                        th.PMT_QUERY,
                        th.PMT_LOVFUNCTION,
                        th.PMT_PROPERTY,
                        th.PMT_UPDATECOUNT,
                        th.PMT_EWSLOVDEF,
                        th.PMT_BOTTEXT,
                        th.PMT_UPDATED,
                        th.PMT_MEKEY,
                        th.PMT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPPARMS as  th ;
                     