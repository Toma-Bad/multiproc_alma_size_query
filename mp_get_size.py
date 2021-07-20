#!/usr/bin/env python3
import numpy as np
from astroquery.alma import Alma
import pickle
from astropy.io import ascii
import time
import multiprocessing as mp
alma = Alma()

#load the full data table from disk
full_data = ascii.read("alma_archive_fil_uniq_trim.txt",format = "fixed_width")

#given a table, line by line get the size of data from alma archive
def get_sizes(table_in,n):
	data_size_list = []
	for ii,data_row in enumerate(table_in):
		start = time.time()
		mousid = data_row['member_ous_uid']
		print(mousid,"% done:",np.round(ii/len(table_in)*100,decimals = 3))
		try:
			#get data from alma archive based on the id of the obs
			data_info = alma.get_data_info(mousid)
			#bitsize shift, to transform from bytes to gigabytes
			data_size = np.sum(data_info['content_length'])>>30
		except Exception as e:
			#if something goes wrong with querying the archive, don't crash
			#make the size zero
			data_size = 0
			print(e)
		data_size_list.append(data_size)
		end = time.time()
		#tell me how long it took to run this step
		dtime = end - start
		#based on the last step, tell me how long i still have
		print(mp.current_process(),"time left (hr):",(len(table_in) - ii)*dtime/3600)
	#append data to input table and write it to disk
	table_in['size_GB'] = data_size_list
	ascii.write(table_in,"mp_arc_size_{:.1f}.txt".format(n),format = "fixed_width")

if __name__ == '__main__':
	processes = []
	for i in range(int(len(full_data)/200)+1):
		#split the big table into smaller tables of 200 lines each
		tab = full_data[i*200:(i+1)*200]
		#run the process on each one
		p = mp.Process(target=get_sizes,args=(tab,i))
		p.start()
		processes.append(p)
	for p in processes:
		p.join()


