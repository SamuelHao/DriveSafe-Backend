# DriveSafe
According to the [government of canada](https://tc.canada.ca/en/road-transportation/motor-vehicle-safety/canadian-motor-vehicle-traffic-collision-statistics-2018), in 2018 alone, motor vehicle collisions were the cause of 152,847 injuries, 1,743 of which were fatal. DriveSafe is a database-reliant platform that acts to promote road safety, in hopes of lowering this number in the coming years. 

## Features
DriveSafe allows users to explore collision and traffic volume data regarding intesections in Vancouver B.C.

Some examples of the features are listed below:
1. View the most dangerous intersections
2. Report a collision at an intersection
3. Visualize the number of collisions and average daily traffic at each intersection using the Google Maps API

## Data
1. [An open source collision information dataset](https://public.tableau.com/profile/icbc#!/vizhome/LowerMainlandCrashes/LMDashboard)
2. [A public traffic volume dataset](https://opendata.vancouver.ca/explore/dataset/intersection-traffic-movement-counts/table/)

## Tech Stack
DriveSafe is an **React** web application with a **Node.js** backend connected to a GCP Cloud SQL **MySQL** database.

The frontend repository can be found [here](https://github.com/emilytao/DriveSafe-Frontend)

## Android App
An accompanying android app was developed by one of our team members and can be found [here](https://github.com/Scowluga/DriveSafe)

## Android App Features
DriveSafe supports an interactive map, similar to Google maps, with two main features: 
1. DriveSafe calculates the "safest" route for drivers using open source collision information and live traffic data. 
2. DriveSafe warns drivers when they are approaching dangerous intersections using text-to-speech.

