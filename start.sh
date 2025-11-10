#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Start the RQ worker in the background
echo "Starting RQ Worker..."
python worker.py &

# Start the Gunicorn web server in the foreground (main process)
echo "Starting Gunicorn Web Server..."
exec gunicorn app:app
