#!/bin/bash

exec python /usr/src/app/air_station_info.py &
exec python /usr/src/app/cctv_data.py &
exec python /usr/src/app/korea_loc.py
