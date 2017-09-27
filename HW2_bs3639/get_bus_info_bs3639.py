#!/usr/bin/env python
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


def get_mta_key(require=True):
	'''Attempts to retrieve an MTA key from the MTAKEY environmental variable
	
	require (bool): throw an error if key is missing (default: True)
	'''
	mta_key = os.getenv('MTAKEY')
	if not mta_key and require:
		raise ValueError('Missing MTA key. Please specify your key as the first command line argument or use the MTAKEY environmental variable.')
	else:
		print('Found your MTA key in your environmental variables.')

	return mta_key



# Parse Args
#######################


# check command line arguments
if len(sys.argv) == 2:
	_, bus_line = sys.argv

	# check environmental variables for mta key
	mta_key = get_mta_key()
	output_csv = '{}.csv'.format(bus_line)

elif len(sys.argv) == 3:
	_, mta_key, bus_line = sys.argv

	# Assume that MTA key has > 2 '-' separated parts
	if len(mta_key.split('-')) <= 2:
		mta_key, bus_line, output_csv = get_mta_key(), mta_key, bus_line
	else:
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
	# 'VehicleMonitoringDetailLevel': 'calls',
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


# Used to fill in any missing values
_NA_ = 'N/A'


def try_to_get(data, keys, default=_NA_):
	'''Attempts to drill into a list/dict. If that fails, it returns a default argument
	
	data (list/dict): the object you want to drill into
	keys (list): the list of keys to use sequentially
	default (any): the value you want returned on failure
	'''
	try:
		for k in keys:
			data = data[k]
		return data
	except (IndexError, KeyError, TypeError):
		return default


# Build dataframe
df = pd.DataFrame([
	[
		journey['VehicleLocation'].get('Latitude', _NA_),
		journey['VehicleLocation'].get('Longitude', _NA_),
		try_to_get(journey, ['MonitoredCall', 'StopPointName', 0]),
		try_to_get(journey, ['MonitoredCall', 'ArrivalProximityText']),
		# try_to_get(journey, ['OnwardCalls', 'OnwardCall', 0, 'StopPointName', 0]), # alternate
		# try_to_get(journey, ['OnwardCalls', 'OnwardCall', 0, 'ArrivalProximityText'])
	]
	for journey in vehicle_journeys
], columns=['Latitude', 'Longitude', 'Stop Name', 'Stop Status'])


# Output Results to File
##########################

df.to_csv(output_csv)

print('Bus information saved to {}.'.format(output_csv))

# with open('data.json', 'w') as f: # cache
# 	json.dump(response, f)



