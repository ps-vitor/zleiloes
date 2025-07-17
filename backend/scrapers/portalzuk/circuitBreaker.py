import  time

class CircuitBreaker:
    """Implementação de Circuit Breaker para evitar sobrecarga do servidor"""
    def __init__(self, max_failures=5, reset_timeout=60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0
    
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