CHECK_DB_AVAILABILITY="
import sys, time
import psycopg2
import config
try:
  for i in range(6):
    with psycopg2.connect(config.CONNECTION) as conn:
      time.sleep(0.5)
  sys.exit(0)
except Exception as e:
  sys.exit(1)
"

# Wait until postgres is up
until python3 -c "$CHECK_DB_AVAILABILITY"; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done

python3 /app/borders_daemon.py&
