
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILESYNCDELETE
                   as                       
                    SELECT
                        t.MSD_PK,
                        t.MSD_TABLENAME,
                        t.MSD_DELETED,
                        t.MSD_RENTITY,
                        t.MSD_KEYS,
                        t.MSD_VALUES,
                        t.MSD_UPDATECOUNT,
                        t.MSD_LASTSAVED,
                        t.MSD_ORG,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILESYNCDELETE as  t
					 union
					        SELECT
                        th.MSD_PK,
                        th.MSD_TABLENAME,
                        th.MSD_DELETED,
                        th.MSD_RENTITY,
                        th.MSD_KEYS,
                        th.MSD_VALUES,
                        th.MSD_UPDATECOUNT,
                        th.MSD_LASTSAVED,
                        th.MSD_ORG,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILESYNCDELETE as  th ;
                     