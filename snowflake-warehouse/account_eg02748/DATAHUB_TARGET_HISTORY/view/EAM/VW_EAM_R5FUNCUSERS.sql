
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUNCUSERS
                   as                       
                    SELECT
                        t.FUS_FUNCTION,
                        t.FUS_USER,
                        t.FUS_SHORTN,
                        t.FUS_PRINTER,
                        t.FUS_BUTFUN1,
                        t.FUS_BUTFUN2,
                        t.FUS_BUTFUN3,
                        t.FUS_BUTFUN4,
                        t.FUS_BUTFUN5,
                        t.FUS_BUTFUN6,
                        t.FUS_BUTNAME1,
                        t.FUS_BUTNAME2,
                        t.FUS_BUTNAME3,
                        t.FUS_BUTNAME4,
                        t.FUS_BUTNAME5,
                        t.FUS_BUTNAME6,
                        t.FUS_BUTICON1,
                        t.FUS_BUTICON2,
                        t.FUS_BUTICON3,
                        t.FUS_BUTICON4,
                        t.FUS_BUTICON5,
                        t.FUS_BUTICON6,
                        t.FUS_NOTES,
                        t.FUS_FILTER,
                        t.FUS_UPDATECOUNT,
                        t.FUS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUNCUSERS as  t
					 union
					        SELECT
                        th.FUS_FUNCTION,
                        th.FUS_USER,
                        th.FUS_SHORTN,
                        th.FUS_PRINTER,
                        th.FUS_BUTFUN1,
                        th.FUS_BUTFUN2,
                        th.FUS_BUTFUN3,
                        th.FUS_BUTFUN4,
                        th.FUS_BUTFUN5,
                        th.FUS_BUTFUN6,
                        th.FUS_BUTNAME1,
                        th.FUS_BUTNAME2,
                        th.FUS_BUTNAME3,
                        th.FUS_BUTNAME4,
                        th.FUS_BUTNAME5,
                        th.FUS_BUTNAME6,
                        th.FUS_BUTICON1,
                        th.FUS_BUTICON2,
                        th.FUS_BUTICON3,
                        th.FUS_BUTICON4,
                        th.FUS_BUTICON5,
                        th.FUS_BUTICON6,
                        th.FUS_NOTES,
                        th.FUS_FILTER,
                        th.FUS_UPDATECOUNT,
                        th.FUS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUNCUSERS as  th ;
                     