
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5GRIDFIELD
                   as                       
                    SELECT
                        t.GFD_FIELDID,
                        t.GFD_GRIDID,
                        t.GFD_BOTFUNCTION,
                        t.GFD_BOTNUMBER,
                        t.GFD_CONTROLTYPE,
                        t.GFD_SCRIPTEVENT,
                        t.GFD_TAGNAME,
                        t.GFD_UPDATECOUNT,
                        t.GFD_TAGPARAMS,
                        t.GFD_AGGFUNC,
                        t.GFD_FIELDTYPE,
                        t.GFD_OCCURRENCE,
                        t.GFD_DEPENDENT,
                        t.GFD_SECENTITY,
                        t.GFD_HEADERLOCATION,
                        t.GFD_UPDATED,
                        t.GFD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5GRIDFIELD as  t
					 union
					        SELECT
                        th.GFD_FIELDID,
                        th.GFD_GRIDID,
                        th.GFD_BOTFUNCTION,
                        th.GFD_BOTNUMBER,
                        th.GFD_CONTROLTYPE,
                        th.GFD_SCRIPTEVENT,
                        th.GFD_TAGNAME,
                        th.GFD_UPDATECOUNT,
                        th.GFD_TAGPARAMS,
                        th.GFD_AGGFUNC,
                        th.GFD_FIELDTYPE,
                        th.GFD_OCCURRENCE,
                        th.GFD_DEPENDENT,
                        th.GFD_SECENTITY,
                        th.GFD_HEADERLOCATION,
                        th.GFD_UPDATED,
                        th.GFD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5GRIDFIELD as  th ;
                     