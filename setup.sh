#!/bin/bash
echo "Setting up python venv..."
python3 -m venv .venv
source ".venv/bin/activate"
python3 -m pip install pandas pyarrow db-dtypes ujson google-cloud-bigquery google-cloud-bigquery-storage wcmatch zstandard

echo "installing Google cloud cli tools..."
sudo apt-get install apt-transport-https ca-certificates gnupg curl -y
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install google-cloud-cli -y

echo "Please login with the account to use for bigquery..."
gcloud auth application-default login
