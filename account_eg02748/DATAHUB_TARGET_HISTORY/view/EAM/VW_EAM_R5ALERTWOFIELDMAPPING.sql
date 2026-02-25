
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTWOFIELDMAPPING
                   as                       
                    SELECT
                        t.AWF_ALERT,
                        t.AWF_WOFIELD,
                        t.AWF_GRIDFIELD,
                        t.AWF_GRIDFIELDDATATYPE,
                        t.AWF_BOILERTEXTNUMBER,
                        t.AWF_UPDATECOUNT,
                        t.AWF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTWOFIELDMAPPING as  t
					 union
					        SELECT
                        th.AWF_ALERT,
                        th.AWF_WOFIELD,
                        th.AWF_GRIDFIELD,
                        th.AWF_GRIDFIELDDATATYPE,
                        th.AWF_BOILERTEXTNUMBER,
                        th.AWF_UPDATECOUNT,
                        th.AWF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTWOFIELDMAPPING as  th ;
                     