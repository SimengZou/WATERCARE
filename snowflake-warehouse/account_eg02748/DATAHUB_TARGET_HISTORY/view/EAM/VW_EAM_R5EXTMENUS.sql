
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EXTMENUS
                   as                       
                    SELECT
                        t.EMN_CODE,
                        t.EMN_SEQUENCE,
                        t.EMN_GROUP,
                        t.EMN_FUNCTION,
                        t.EMN_PARENT,
                        t.EMN_TYPE,
                        t.EMN_UPDATECOUNT,
                        t.EMN_MEFLAG,
                        t.EMN_HIDE,
                        t.EMN_MOBILE,
                        t.EMN_ICON,
                        t.EMN_LASTSAVED,
                        t.EMN_TRANSIT,
                        t.EMN_DUXMOBILE,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EXTMENUS as  t
					 union
					        SELECT
                        th.EMN_CODE,
                        th.EMN_SEQUENCE,
                        th.EMN_GROUP,
                        th.EMN_FUNCTION,
                        th.EMN_PARENT,
                        th.EMN_TYPE,
                        th.EMN_UPDATECOUNT,
                        th.EMN_MEFLAG,
                        th.EMN_HIDE,
                        th.EMN_MOBILE,
                        th.EMN_ICON,
                        th.EMN_LASTSAVED,
                        th.EMN_TRANSIT,
                        th.EMN_DUXMOBILE,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EXTMENUS as  th ;
                     