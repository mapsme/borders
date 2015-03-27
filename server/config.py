# postgresql connection string
CONNECTION = 'dbname=borders'
# passed to flask.Debug
DEBUG = False
# if the main table is read-only
READONLY = False
# main table name
TABLE = 'borders'
# from where OSM borders are imported
OSM_TABLE = 'osm_borders'
# tables with borders for reference
OTHER_TABLES = { 'old': 'old_borders' }
# backup table
BACKUP = 'borders_backup'
# area of an island for it to be considered small
SMALL_KM2 = 10
# force multipolygons in JOSM output
JOSM_FORCE_MULTI = True
# alert instead of json on import error
IMPORT_ERROR_ALERT = False
# file to which daemon writes the name of currently processed region
DAEMON_STATUS_PATH = '/var/www/html/borders-daemon-status.txt'
