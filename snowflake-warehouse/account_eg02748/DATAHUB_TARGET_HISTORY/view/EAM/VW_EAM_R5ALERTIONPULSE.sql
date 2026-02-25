
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTIONPULSE
                   as                       
                    SELECT
                        t.ALI_ALERT,
                        t.ALI_DELAY,
                        t.ALI_DELAYUOM,
                        t.ALI_RECIPIENTFIELDID,
                        t.ALI_DESCRIPTIONFIELDID,
                        t.ALI_UPDATECOUNT,
                        t.ALI_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTIONPULSE as  t
					 union
					        SELECT
                        th.ALI_ALERT,
                        th.ALI_DELAY,
                        th.ALI_DELAYUOM,
                        th.ALI_RECIPIENTFIELDID,
                        th.ALI_DESCRIPTIONFIELDID,
                        th.ALI_UPDATECOUNT,
                        th.ALI_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTIONPULSE as  th ;
                     