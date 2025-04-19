# big-data-assignment2-2025

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose

## Step 2: Check the app/data/ folder
- Make sure your a.parquet file is in app/data/ folder

## Step 3: Run the command
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.