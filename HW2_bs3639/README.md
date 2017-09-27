# Homework 2

I worked alone on all of the assignments.

## Assignment 1
This script, `show_bus_locations_bs3639.py`, prints out information about the buses running on the Bus Line specified in the 2nd command line argument.

*Usage*:
```
show_bus_locations_bs3639.py <MTA_KEY> <BUS_LINE>
```
Or if you have your MTAKEY saved as an enviromental variable, MTAKEY, you can use:
```
show_bus_locations_bs3639.py <BUS_LINE>
```

The file can be found [here](show_bus_locations_bs3639.py).

## Assingnment 2
This script outputs the information about the line's next stops in a csv file.

*Usage*:
```
get_bus_info_bs3639.py <MTA_KEY> <BUS_LINE> <OUTPUT_FILE>
```
Or if you have your MTAKEY saved as an enviromental variable, MTAKEY, you can use:
```
get_bus_info_bs3639.py <BUS_LINE> <OUTPUT_FILE>
```
If the output file is not specified, it will default to `<BUS_LINE>.csv`:
```
get_bus_info_bs3639.py <BUS_LINE>
```
If your first argument has multiple dashes in it, it is assumed to be an MTA key:
```
get_bus_info_bs3639.py <MTA_KEY> <BUS_LINE>
```

MTA's SIRI documentation says that its next stop information is stored in the `MonitoredCall` key of the `MonitoredVehicleJourney` object.

The [MTA docs](http://bustime.mta.info/wiki/Developers/SIRIMonitoredVehicleJourney) say:
> &lt;!-- Call data about a particular stop --&gt;
&lt;!-- In StopMonitoring, it is the stop of interest; in VehicleMonitoring it is the next stop the bus will make. --&gt;
&lt;MonitoredCall&gt;
...



The file can be found [here](get_bus_info_bs3639.py).

## Assignment 3
The purpose of this assignment is to display data from the CUSP data facility in both tabular and graphical format. The data source used is NYC Department of Education Job Titles, accessible via the slug, [`s7yj-m732/1414245680s7yj-m732`](http://urbanprofiler.cloudapp.net/dataset/s7yj-m732/). 

The notebook can be found [here](HW2_A3.ipynb).

## Extra Credit
The purpose of this assignment was to do the same thing as Assignment 3, but handle datetime objects. The data source used is the 2005 Campaign Expenditures. The slug for the data source is [`kwmq-dbub/1414245703/kwmq-dbub`](http://urbanprofiler.cloudapp.net/dataset/easq-ubfe/).

The notebook can be found [here](HW2_Extra_Credit.ipynb).
