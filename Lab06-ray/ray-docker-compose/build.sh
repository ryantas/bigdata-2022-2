#!/usr/bin/env bash
set -e
apt-get update
apt-get -y install build-essential
echo "Getting Python packages..."
/usr/local/bin/python -m pip install --upgrade pip
pip install -U --no-cache-dir -r requirements.txt
rm requirements.txt
echo "Done!"