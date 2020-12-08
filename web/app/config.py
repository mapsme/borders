# postgresql connection string
CONNECTION = 'dbname=borders user=borders password=borders host=dbhost port=5432'
# passed to flask.Debug
DEBUG = True
# if the main table is read-only
READONLY = False
# main table name
BORDERS_TABLE = 'borders'
# from where OSM borders are imported
OSM_TABLE = 'osm_borders'
# All populated places in OSM
OSM_PLACES_TABLE = 'osm_places'
# transit table for autosplitting results
AUTOSPLIT_TABLE = 'splitting'
# tables with borders for reference
OTHER_TABLES = {
    #'old': 'old_borders'
}
# backup table
BACKUP = 'borders_backup'
# area of an island for it to be considered small
SMALL_KM2 = 10
# force multipolygons in JOSM output
JOSM_FORCE_MULTI = True
# alert instead of json on import error
IMPORT_ERROR_ALERT = False
# file to which daemon writes the name of currently processed region
DAEMON_STATUS_PATH = '/tmp/borders-daemon-status.txt'
DAEMON_PID_PATH = '/tmp/borders-daemon.pid'
DAEMON_LOG_PATH = '/var/log/borders-daemon.log'
# mwm size threshold in Kb
MWM_SIZE_THRESHOLD = 70*1024
# Estimated mwm size is predicted by the 'model.pkl' with 'scaler.pkl' for X
MWM_SIZE_PREDICTION_MODEL_PATH = '/app/data/model.pkl'
MWM_SIZE_PREDICTION_MODEL_SCALER_PATH = '/app/data/scaler.pkl'
