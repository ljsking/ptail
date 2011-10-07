
import sys
from datetime import datetime

import argparse
import logging

parser = argparse.ArgumentParser(description='Validate ptail output.')
parser.add_argument('--logfile', 
                   default='validator.log',)
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG,
	filename=args.logfile,
	filemode='w',
	format='%(asctime)s %(levelname)s %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
#logging.getLogger('').addHandler(console)

before = 0
monitoring_start = datetime.now()

read_count = 0
summed_elapsed = 0

for line in sys.stdin:
	try:
		line = line.strip()
		tokens = line.split()
		time_tokens = ' '.join(tokens[0:2]).split('.')
		worker = tokens[2]
		line_id = int(tokens[3])
		read_count += 1

		#if before+1 != line_id:
			#logging.error("before: %d now: %d"%(before, line_id))

		before = line_id
		logged_time = datetime.strptime(time_tokens[0], '%Y-%m-%d %H:%M:%S')
		if len(time_tokens) > 1:
			logged_time.replace(microsecond=int(time_tokens[1]))
		delta = datetime.now() - logged_time
		summed_elapsed += delta.seconds
		#if 2 < delta.seconds:
			#logging.error("over 2 seconds %d"%delta.seconds)

		if 10 < (datetime.now() - monitoring_start).seconds:
			monitoring_start = datetime.now()
			logging.debug("now processing %s"%logged_time)
			logging.debug("monitoring-data(totalcntFor10secs,tps,elpasedtime) %d,%d,%f"
				%(read_count, read_count/10, float(summed_elapsed)/read_count))
			read_count = 0
			summed_elapsed = 0
				
				#print >>sys.stderr, delta, worker, line_id
	except ValueError:
		logging.exception('parse error %s'%' '.join(tokens[0:2]))
	except:
		#pass
		logging.exception('Raise exception')
