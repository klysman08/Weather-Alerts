# Weather conditions and forecasts across different regions of the United States
The National Weather Service (NWS) API allows access to critical forecasts, alerts, and observations, along with other weather data.


## Goal
In this project you will create a data pipeline that ingests wather warnings

Input: https://alerts.weather.gov/cap/us.php?x=0 fips6 location codes to translate warining location into region, state and county

- Create a bronze table to where every batch of data is appended
- Create a silver table where waning data is merge
- Create a gold table where warning location is enriched
The pipeline should both consider initial and incremental loads

Counts by state, county, date, type of alert
What alerts are currently active
## Dataflows
- get alerts, get fips -> select and filters alerts -> marge datasets geocode -> Plots
