import os
import monetdb.sql
import datetime
import random

# query_prefs = {
# 	'ts_latest':30,
# 	'ts_id_time':20,
# 	'ts_time':10,
# 	'ts_near_time':10
# }

query_args={
	'interest_id':9808,	#[1,218743]
	'time_center':2455299.5022,	#[2455299.5,2455299.916667]
	'time_window':0.001,
	'space_ra_window':2,	#(1,13.72)
	'space_dec_window':2	#(1,13.27)
}

random_args={
	'id_step':4000,
	'time_step':0.01
}

def run_workflow(query_args):
	for i in range(1,10000):
		interest_id_arg=(query_args['interest_id']+int(random.expovariate(1/1))*random_args['id_step'])%218743
		time_center_arg=query_args['time_center']+int(random.expovariate(1/1))*random_args['time_step']
		os.system('echo "----the %dth workflow. interest_id: %d, time_center:%f---"' %(i,interest_id_arg,time_center_arg))
		#1.ts_latest
		os.system('echo "1.ts_latest begin"')
		starttime = datetime.datetime.now()
		os.system('mclient gwacdb -e -s "select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i where a.targetid=t.id and t.imageid=i.imageid and i.imageid=2400" |grep tuples')
		endtime = datetime.datetime.now()
		os.system('echo "ts_latest end, time cost: %s"' %(endtime-starttime))
		#2.ts_id_time
		os.system('echo "2.ts_id_time begin"')
		starttime = datetime.datetime.now()
		os.system('mclient gwacdb -e -s "select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i where a.uniqueid=%d and a.targetid=t.id and t.imageid=i.imageid and i.jd>%f and i.jd<%f" |grep tuples' %(interest_id_arg,time_center_arg-query_args['time_window'],time_center_arg+query_args['time_window']))
		endtime = datetime.datetime.now()
		os.system('echo "ts_id_time end, time cost: %s"' %(endtime-starttime))
		#3.ts_time
		os.system('echo "3.ts_time begin"')
		starttime = datetime.datetime.now()
		os.system('mclient gwacdb -e -s "select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i where a.targetid=t.id and t.imageid=i.imageid and i.jd>%f and i.jd<%f" |grep tuples' %(time_center_arg-query_args['time_window'],time_center_arg+query_args['time_window']))
		endtime = datetime.datetime.now()
		os.system('echo "ts_time end, time cost: %s"' %(endtime-starttime))
		#4.ts_near_time
		os.system('echo "4.ts_near_time begin"')
		starttime = datetime.datetime.now()
		connection = monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="gwacdb")
		cursor = connection.cursor()
		cursor.execute("select ra_avg,decl_avg from uniquecatalog18 where id=%d" %interest_id_arg)
		address=cursor.fetchone()
		os.system('mclient gwacdb -e -s "select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i where a.targetid=t.id and t.imageid=i.imageid and i.jd>%f and i.jd<%f and uniqueid in (select id from uniquecatalog18 where ra_avg>%f and ra_avg<%f and decl_avg>%f and decl_avg<%f)" |grep tuples' %(time_center_arg-query_args['time_window'],time_center_arg+query_args['time_window'],address[0]-query_args['space_ra_window'],address[0]+query_args['space_ra_window'],address[1]-query_args['space_dec_window'],address[1]+query_args['space_dec_window']))
		endtime = datetime.datetime.now()
		os.system('echo "ts_near_time end, time cost: %s"' %(endtime-starttime))

os.system('mclient gwacdb -e -s "call querylog_enable()"')
run_workflow(query_args)

#prepare:create time series table:tmp_ts
	#os.system('mclient gwacdb -e -s "drop table sim_ts"')
	#os.system('mclient gwacdb -e -s "create table sim_ts as select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i where a.targetid=t.id and t.imageid=i.imageid"')

# select a.uniqueid,i.jd,t.flux from associatedsource18 a, targets18 t, image3 i
# where a.uniqueid=1 and a.targetid=t.id and t.imageid=i.imageid;
# and ra >234 and ra<234.05 and i.jd>2455299.5 and i.jd<2455299.6;

# --High Volume 3-Spatially-restricted and time filter: a square degree box in the sky 233.14332222222075<=ra<=246.86518253839938, 3.3549236162361598=<dec<=16.627396174863318
# --mclient gwacdb -e -s "select count(*) from targets18 t where ra >234 and ra<235 and dec>4 and dec<5;"
# select count(*)--uniqueid,i.jd,t.flux
# from associatedsource18 a, targets18 t, image3 i
# where a.targetid=t.id and t.imageid=i.imageid and ra >234 and ra<234.05 and i.jd>2455299.5 and i.jd<2455299.6;

# --very high: neaibor filter. todo:replace uniquecatalog18 to targets18
# select count(*)
# from uniquecatalog18 u1, uniquecatalog18 u2, associatedsource18 a
# where u1.id=1 and u2.ra_avg<(u1.ra_avg+0.05) and u2.decl_avg<(u1.decl_avg+0.05) and u2.id=a.uniqueid;
# }