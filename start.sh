#!/usr/bin/env bash

# FIX: Ensure script has executable permissions
chmod +x start.sh 

# Exit immediately if a command exits with a non-zero status.
set -e

# Start the RQ worker in the background (using python to find the script)
echo "Starting RQ Worker in background..."
python worker.py &

# Start the Gunicorn web server in the foreground (this is the main process)
echo "Starting Gunicorn Web Server..."
exec gunicorn app:app --bind 0.0.0.0:$PORT
