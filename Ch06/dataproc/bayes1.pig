REGISTER /usr/lib/pig/piggybank.jar ;

FLIGHTS = LOAD 'gs://$PROJECT_NAME/flights/tzcorr/flights-*'
    using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE')
    AS (
    FL_DATE:chararray,
    OP_UNIQUE_CARRIER:chararray,
    OP_CARRIER_AIRLINE_ID:chararray,
    OP_CARRIER:chararray,
    OP_CARRIER_FL_NUM:chararray,
    ORIGIN_AIRPORT_ID:chararray,
    ORIGIN_AIRPORT_SEQ_ID:int,
    ORIGIN_CITY_MARKET_ID:chararray,
    ORIGIN:chararray,
    DEST_AIRPORT_ID:chararray,
    DEST_AIRPORT_SEQ_ID:int,
    DEST_CITY_MARKET_ID:chararray,
    DEST:chararray,
    CRS_DEP_TIME:datetime,
    DEP_TIME:datetime,
    DEP_DELAY:float,
    TAXI_OUT:float,
    WHEELS_OFF:datetime,
    WHEELS_ON:datetime,
    TAXI_IN:float,
    CRS_ARR_TIME:datetime,
    ARR_TIME:datetime,
    ARR_DELAY:float,
    CANCELLED:chararray,
    CANCELLATION_CODE:chararray,
    DIVERTED:chararray,
    DISTANCE:float,
    DEP_AIRPORT_LAT:float,
    DEP_AIRPORT_LON:float,
    DEP_AIRPORT_TZOFFSET:float,
    AR_AIRPORT_LAT:float,
    ARR_AIRPORT_LON:float,
    ARR_AIRPORT_TZOFFSET:float,
    EVENT:chararray,
    NOTIFY_TIME:datetime) ;

FLIGHTS2 = FOREACH FLIGHTS GENERATE
(DISTANCE < 214? 0:
(DISTANCE < 316? 1:
(DISTANCE < 394? 2:
(DISTANCE < 493? 3:
(DISTANCE < 596? 4:
(DISTANCE < 731? 5:
(DISTANCE < 868? 6:
(DISTANCE < 1020? 7:
(DISTANCE < 1269? 8: 9))))))))) AS distbin:int,
(DEP_DELAY < -8? 0:
(DEP_DELAY < -6? 1:
(DEP_DELAY < -4? 2:
(DEP_DELAY < -3? 3:
(DEP_DELAY < -2? 4:
(DEP_DELAY < 0? 5:
(DEP_DELAY < 2? 6:
(DEP_DELAY < 6? 7:
(DEP_DELAY < 13? 8: 9))))))))) AS depdelaybin:int, (ARR_DELAY < 15? 1:0) AS ontime:int;

grouped = GROUP FLIGHTS2 BY (distbin, depdelaybin);
result = FOREACH grouped GENERATE
    FLATTEN(group) AS (dist, delay),
    ((double)SUM(FLIGHTS2.ontime))/COUNT(FLIGHTS2.ontime) AS ontime:double;

STORE result into 'gs://$PROJECT_NAME/flights/pigoutput/' using PigStorage(',', '-schema');
