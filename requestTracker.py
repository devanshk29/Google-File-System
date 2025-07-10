from time import time, sleep
import threading    
from collections import deque

class RequestTracker:
    def __init__(self, window_size=15):
        self.window_size = window_size  # Time window in seconds
        self.requests = deque()        # Stores timestamps of requests
        self.lock = threading.Lock()   # For thread safety

    def add_request(self):
        print("Adding request")
        with self.lock:
            current_time = time()
            self.requests.append(current_time)
            self._cleanup(current_time)

    def get_request_count(self):
        with self.lock:
            current_time = time()
            self._cleanup(current_time)
            return len(self.requests)
    def get_count(self):
        # with self.lock:
        current_time = time()
        # self._cleanup(current_time)
        return len(self.requests)
    
    def _cleanup(self, current_time):
        # Remove requests that are older than the window size
        while self.requests and self.requests[0] < current_time - self.window_size:
            self.requests.popleft()
