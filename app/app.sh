#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh

# # Run the indexer
bash index.sh

# # Run the ranker
bash search.sh "my query"
# run to test the correctness (I extractes these words from 1 particular file "A Different Pond"):
bash search.sh "asian family page memior kirkus krueger bao phi"
bash search.sh "Tim Chamberlain, a doctoral candidate at Birkbeck, University of London wrote in his review for the London School"
tail -f /dev/null