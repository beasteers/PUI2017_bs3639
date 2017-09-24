from __future__ import print_function
import sys

# check command line arguments
if len(sys.argv) == 3:
	_, mta_key, bus_line = sys.argv
else:
	raise ValueError('Please enter both your MTA key and the bus line. e.g. show_bus_locations.py xx...xx B52')










print('Bus Line: {}'.format(bus_line))
print('Number of Active Buses: {}'.format(n_buses))

for i, bus in enumerate(buses):
	print('Bus {} is at {} latitude and {} longitude')