from __future__ import print_function
import pandas as pd
import sys
import os
import json

# try Python 3 version, fallback to Python 2 version
try:
    from urllib.request import urlopen
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlopen
    from urllib import urlencode


# Parse Args
#######################

# check command line arguments
if len(sys.argv) == 2:
	_, bus_line = sys.argv

	# check environmental variables for mta key
	print('Checking for MTA key in environmental variables')
	
	mta_key = os.getenv('MTAKEY')
	if not mta_key:
		raise ValueError('Missing MTA key. Please specify your key as the first command line argument or use the MTAKEY environmental variable.')
	
	print('Found!')

elif len(sys.argv) == 3:
	_, mta_key, bus_line = sys.argv

else:
	raise ValueError('Please enter both your MTA key and the bus line. e.g. show_bus_locations.py xx...xx B52')

# make cli arg case-insensitive
bus_line = bus_line.upper()





# Get Request
#######################

# build the request
url = 'http://bustime.mta.info/api/siri/vehicle-monitoring.json'
url += '?' + urlencode({
	'key': mta_key,
	'version': 2,
	'LineRef': bus_line
})

# get the response
response = json.loads(urlopen(url).read().decode('utf-8'))
# with open('data.json', 'r') as f: # cached
# 	response = json.load(f)





# Output Results
#######################

# Get the list of buses
vehicle_activity = response['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']

# Print results
print('')
print('Bus Line: {}'.format(bus_line))
print('Number of Active Buses: {}'.format(len(vehicle_activity)))

for i, act in enumerate(vehicle_activity):
	loc = act['MonitoredVehicleJourney']['VehicleLocation']
	print('Bus {} is at {} latitude and {} longitude'.format(i, loc['Latitude'], loc['Longitude']))

# with open('data.json', 'w') as f: # cache
# 	json.dump(response, f)




