#!/bin/bash
set -e
OSMFILTER=osmfilter
OSMCONVERT=osmconvert

if [[ ! -r "$PLANET" ]]; then
	echo "Error: planet file cannot be found or read."
	exit 1
fi

if [ ! -x `which $OSMFILTER` ]; then
	echo "No osmfilter found."
	exit 1
fi

if [ ! -x `which $OSMCONVERT` ]; then
	echo "No osmconvert found."
	exit 1
fi

# 1. Filter planet file, leaving only administrative borders (and cities)
echo Filtering planet
if [[ "$PLANET" != *.o5m ]]; then
	CONVERTED_PLANET=${PLANET}.o5m
	$OSMCONVERT $PLANET --out-o5m -o=$CONVERTED_PLANET
else
	CONVERTED_PLANET=$PLANET
fi

$OSMFILTER $CONVERTED_PLANET --keep="boundary=administrative or place=" --out-o5m -o=$FILTERED_PLANET || exit 3

chmod +r $FILTERED_PLANET

