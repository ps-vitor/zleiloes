# circuit_breaker.py
import time
from    functools   import  wraps

class CircuitBreaker:
    def __init__(self, max_failures=5, reset_timeout=60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0

    def decorator(self):
        def wrapper(f):
            @wraps(f)
            def wrapped(*args, **kwargs):
                # Circuit breaker logic here
                if self.failure_count >= self.max_failures:
                    if time.time() - self.last_failure_time < self.reset_timeout:
                        raise Exception("Circuit breaker is open")
                    else:
                        self.failure_count = 0  # Reset after timeout
                
                try:
                    result = f(*args, **kwargs)
                    self.failure_count = 0  # Reset on success
                    return result
                except Exception as e:
                    self.failure_count += 1
                    self.last_failure_time = time.time()
                    raise
            return wrapped
        return wrapper

    def record_failure(self):
        """Registra uma falha e ativa o circuit breaker se necessário"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.max_failures:
            print(f"Circuit breaker tripped! Waiting {self.reset_timeout} seconds before retrying.")
            time.sleep(self.reset_timeout)
            self.reset()
    
    def reset(self):
        """Reseta o contador de falhas"""
        self.failure_count = 0
        self.last_failure_time = 0