import  requests,random,time

class RequestManager:
    def __init__(self, user_agents, circuit_breaker):
        self.user_agents = user_agents
        self.circuit_breaker = circuit_breaker
        self._session = self._create_session()  # Atributo privado
    
    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            # Outros headers...
        })
        return session
    
    @property
    def session(self):
        """Expõe a sessão através de uma property"""
        return self._session
    
    def random_delay(self):
        # Implementação do delay
        time.sleep(random.uniform(1, 3))