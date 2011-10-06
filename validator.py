
import sys
from datetime import datetime

import logging

before = 0

monitoring_start = datetime.now()


read_count = 0
summed_elapsed = 0

for line in sys.stdin:
	try:
		line = line.strip()
		tokens = line.split()
		time_token, micro_secs = ' '.join(tokens[0:2]).split('.')
		worker = tokens[2]
		line_id = int(tokens[3])
		read_count += 1

		if before+1 != line_id:
			print >>sys.stderr, before, line_id

		before = line_id
		logged_time = datetime.strptime(time_token, '%Y-%m-%d %H:%M:%S')
		logged_time.replace(microsecond=int(micro_secs))
		delta = datetime.now() - datetime.strptime(time_token, '%Y-%m-%d %H:%M:%S')
		summed_elapsed += delta.microseconds
		if 1000000 < delta.microseconds:
			print >>sys.stderr, "over 1 seconds", delta.microseconds

		if 10 < (datetime.now() - monitoring_start).seconds:
			monitoring_start = datetime.now()
			print >>sys.stderr, "total counts", read_count
			print >>sys.stderr, "avg elapsed %f ms"%(summed_elapsed/read_count/1000.)
			read_count = 0
			summed_elapsed = 0
				
				#print >>sys.stderr, delta, worker, line_id
	except:
		#pass
		logging.exception('Raise exception')
		print >>sys.stderr, "parse error"
