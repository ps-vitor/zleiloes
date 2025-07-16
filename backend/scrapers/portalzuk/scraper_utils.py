import csv
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class   Utils:
    def _create_requests_session(self):
        """Cria e configura uma nova sessão requests com retry e User-Agent rotativo."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5, # Reduzido o número de retries para evitar esperas muito longas em falhas persistentes
            backoff_factor=1.5, # Backoff ligeiramente menor
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.portalzuk.com.br/',
            'User-Agent': random.choice(self.user_agents) # Rotaciona User-Agent para cada nova sessão
        })
        return session

    def random_delay(self):
        """Adiciona atraso aleatório e mais variável entre requisições para evitar ser bloqueado."""
        time_since_last = time.time() - self.last_request_time
        # Aumenta a variabilidade e o tempo mínimo de espera
        sleep_time = random.uniform(self.min_request_interval, self.min_request_interval + self.max_request_interval_addition)
        if time_since_last < sleep_time:
            time.sleep(sleep_time - time_since_last)
        self.last_request_time = time.time()

    def is_valid_url(self, url):
        """Verifica se a URL é válida."""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False