#!/bin/bash

# Run 8 instances of chunk_server.py and provide input for ports
for port in {5021..5028}
do
    echo "Starting chunk_server.py for port $port..."
    
    # Start the Python script and pass the port number as input
    (echo $port | python3 chunk_server.py) &
done

# Wait for all background processes to complete (optional)
wait

echo "All chunk_server.py instances have been started with their respective ports."
