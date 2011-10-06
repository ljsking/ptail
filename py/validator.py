
import sys
from datetime import datetime

import logging

logging.basicConfig(level=logging.DEBUG,
	format='%(asctime)s %(levelname)s %(message)s')

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
			logging.error("before: %d now: %d"%(before, line_id))

		before = line_id
		logged_time = datetime.strptime(time_token, '%Y-%m-%d %H:%M:%S')
		logged_time.replace(microsecond=int(micro_secs))
		delta = datetime.now() - logged_time
		summed_elapsed += delta.seconds
		#if 2 < delta.seconds:
			#logging.error("over 2 seconds %d"%delta.seconds)

		if 10 < (datetime.now() - monitoring_start).seconds:
			monitoring_start = datetime.now()
			logging.debug("now processing %s"%logged_time)
			logging.debug("total counts %d"%read_count)
			logging.debug("avg elapsed %f sec"%(float(summed_elapsed)/read_count))
			read_count = 0
			summed_elapsed = 0
				
				#print >>sys.stderr, delta, worker, line_id
	except ValueError:
		logging.exception('parse error %s'%' '.join(tokens[0:2]))
	except:
		#pass
		logging.exception('Raise exception')
