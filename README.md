# CoDe-Testbed
Personal project working under the supervision of Dr. Paul Grogan

# River Node Simulator
This script was developed to work in conjunction with three other scripts designed by my colleagues: A script simulation a weather station, and two scripts simulating satellites.  

# Goal
The goal of the project was to develop a discrete time simulation to simulate intercommunication between sensors during an emergency scenario, such as a flood. Data pulled my one sensor is sent to a testbed server using paho mqtt, and can be used by another sensor to change operating mode. For example, my script simulates a streamgage that takes waterlevel and flow rate measurements from a river every 15 minutes. If my colleague's weather station node detects heavy rainfall, I can retreive that information from the testbed, and decrease my measurement intervals to every 10 minutes in order to keep better track of water levels during a potential flooding scenario. Furthermore, if the water level of the river rises over a predetermined threshold, then my node decreases measurement intervals to every 5 minutes for more frequent tracking during a flood. 

# How it works
Once the script is run, it waits for an activation message from the control channel using the mqtt connection to "testbed.codelab.org" (see "msg-syntax" file for more information on how messages are structured). The activation message specifies the simulation start and end date and time, the speed of the simulation, and the actual start time of the simulation (ie 45 seconds from receiving the message).

Using the simulation start and end date/time, the script pulls streamgage data from https://waterservices.usgs.gov corresponding to the correct time. This data contains measurements taken every 15 minutes, so the script interpolates between the data points in order to allow me to simulate decreasing measurement intervals to every 5 minutes when necessary.

When the water level reaches a pre-determined flood threshold, the script sends out a message containing a flood warning to be used my colleagues
