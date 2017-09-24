from __future__ import print_function
import sys
import os
import json
import pandas as pd

# try Python 3 version, fallback to Python 2 version
try:
    from urllib.request import urlopen
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlopen
    from urllib import urlencode


"""
# you can specify your mta key, the bus line, and the output file
python get_bus_info.py [mta_key] [bus_line] [output_filename]

# or you can leave out the output file
python get_bus_info.py [mta_key] [bus_line] 
	[output_filename=<bus_line>.csv]

# and if you have your mta key saved as an environmental variable you can leave that out too
python get_bus_info.py [bus_line] 
	[mta_key=os.getenv('MTAKEY')]
	[output_filename=<bus_line>.csv]

# the case where your mta key is an env var, but you want a different output filename is not covered. Sorry.
"""




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

	output_csv = '{}.csv'.format(bus_line)

elif len(sys.argv) == 3:
	_, mta_key, bus_line = sys.argv
	output_csv = '{}.csv'.format(bus_line)

elif len(sys.argv) == 4:
	_, mta_key, bus_line, output_csv = sys.argv

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


# Get the list of bus data
vehicle_journeys = [
	act['MonitoredVehicleJourney'] 
	for act in response['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
]

_NA_ = 'N/A'

# Build dataframe
df = pd.DataFrame([
	[
		journey['VehicleLocation'].get('Latitude', _NA_),
		journey['VehicleLocation'].get('Longitude', _NA_),
		journey['DestinationName'][0] if len(journey['DestinationName']) else _NA_,
		journey['MonitoredCall'].get('ArrivalProximityText', _NA_)
	]
	for journey in vehicle_journeys
], columns=['Latitude', 'Longitude', 'Stop Name', 'Stop Status'])

# Output Results to File
##########################

df.to_csv(output_csv)




