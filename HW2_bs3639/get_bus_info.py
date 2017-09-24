from __future__ import print_function
import sys

# check command line arguments
if len(sys.argv) == 3:
	_, mta_key, bus_line = sys.argv
	output_csv = '{}.csv'.format(bus_line)
if len(sys.argv) == 4:
	_, mta_key, bus_line, output_csv = sys.argv
else:
	raise ValueError('Please enter both your MTA key and the bus line. e.g. show_bus_locations.py xx...xx B52')








df.to_csv(output_csv)