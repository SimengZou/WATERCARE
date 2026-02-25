create or replace view VW_ICARE_LOAD_LOG(
	ETL_LOAD_METADATAFILENAME,
	TABLECOUNT,
	ETL_LOAD_DATETIME
) as 
select 
 c.ETL_LOAD_METADATAFILENAME
 ,c.SRC:result:stats:count::number as tablecount
 ,c.ETL_LOAD_DATETIME
from DATAHUB_LOGGING.ICARE_API_COUNT  c join (select 
                                                max(ETL_LOAD_DATETIME) as ETL_LOAD_DATETIME
                                                ,ETL_LOAD_METADATAFILENAME 
                                                from
                                                DATAHUB_LOGGING.ICARE_API_COUNT
                                                   group by
                                                    ETL_LOAD_METADATAFILENAME
                                               ) as  maxe 
on maxe.ETL_LOAD_DATETIME=c.ETL_LOAD_DATETIME and maxe.ETL_LOAD_METADATAFILENAME=c.ETL_LOAD_METADATAFILENAME
where
1=1
order by c.ETL_LOAD_DATETIME desc;