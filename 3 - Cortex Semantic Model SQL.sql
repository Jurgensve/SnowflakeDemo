create or replace semantic view NYC_TAXI.YELLOW_TAXI.YELLOW_TRIP_ANALYSIS
	tables (
		PAYMENTS as DIM_TBL_PAYMENTS primary key (PAYMENT_CODE),
		RATES as DIM_TBL_RATES primary key (RATE_CODE),
		TRIPS as FCT_YELLOW_TRIPS primary key (VENDORID,PICKUP_TIME) comment='This table contains detailed information about yellow taxi trips such as amount charged, time of trip and number of passengers. ',
		VENDORS as DIM_TBL_VENDORS primary key (VENDOR_CODE),
		ZONES as DIM_TBL_ZONES primary key (LOCATIONID)
	)
	relationships (
		TRIP_TO_PAYMENT as TRIPS(PAYMENT_TYPE) references PAYMENTS(PAYMENT_CODE),
		TRIP_TO_RATE as TRIPS(RATECODEID) references RATES(RATE_CODE),
		TRIP_TO_VENDOR as TRIPS(VENDORID) references VENDORS(VENDOR_CODE),
		TRIP_TO_DROPOFF_ZONE as TRIPS(DROPOFF_ZONE) references ZONES(LOCATIONID),
		TRIP_TO_PICKUP_ZONE as TRIPS(PICKUP_ZONE) references ZONES(LOCATIONID)
	)
	facts (
		TRIPS.AIRPORT_FEE as AIRPORT_FEE with synonyms=('Airport charge','Airport fee') comment='part of the trip fare that is charged for being transported to the airport.',
		TRIPS.CONGESTION_SURCHARGE as CONGESTION_SURCHARGE with synonyms=('congestion charge','Congestion fee','Congestion surcharge','congestions charges.','traffic fee') comment='Surcharge that is charged due to congestion.',
		TRIPS.FARE_AMOUNT as FARE_AMOUNT with synonyms=('Fare Amount','Trip Fare') comment='The fare amount for the trip excluding tax, surecharges, tip and other extra fees. ',
		TRIPS.TOTAL_CHARGES as FARE_AMOUNT + EXTRA + MTA_TAX + TIP_AMOUNT + TOLLS_AMOUNT + IMPROVEMENT_SURCHARGE + CONGESTION_SURCHARGE + AIRPORT_FEE + CBD_CONGESTION_FEE with synonyms=('Total Charges','Total fair amount','Total Fare') comment='Total charge is calculated as a sum of fare amount, extra charges, tax, tip, airport fee, surcharges for congestion',
		TRIPS.TRIP_ID as CONCAT(VENDORID, '-', PICKUP_TIME)
	)
	dimensions (
		PAYMENTS.PAYMENT_DESCRIPTION as PAYMENT_DESCRIPTION,
		RATES.RATE_DESCRIPTION as RATE_DESCRIPTION,
		TRIPS.DROPOFF_TIME as DROPOFF_TIME with synonyms=('drop off time','Dropoff time','trip end time.') comment='The time that the passanger was dropped off or the time that the trip ended. ',
		TRIPS.PICKUP_DATE as DATE(PICKUP_TIME),
		TRIPS.PICKUP_HOUR as HOUR(PICKUP_TIME),
		TRIPS.PICKUP_TIME as PICKUP_TIME with synonyms=('Pickup time','trip start time') comment='The time that the passanger was picked up or the time that the trip started. ',
		VENDORS.VENDOR_NAME as VENDOR_DESCRIPTION,
		ZONES.PICKUP_BOROUGH as zones.BOROUGH,
		ZONES.ZONE as ZONE with synonyms=('Area','Zone') comment='An area or zone within a borough.'
	)
	metrics (
		TRIPS.AVERAGE_DISTANCE as AVG(TRIP_DISTANCE),
		TRIPS.AVERAGE_FARE as AVG(FARE_AMOUNT),
		TRIPS.AVERAGE_TIP as AVG(TIP_AMOUNT),
		TRIPS.NUMBER_OF_TRIPS as COUNT(*) with synonyms=('Number of trips') comment='Count or number of trips.',
		TRIPS.PASSENGER_COUNT as SUM(PASSENGER_COUNT) with synonyms=('Number of passengers','Passengers','Total Passengers') comment='Total number of passengers in the taxi for the trip'
	)
