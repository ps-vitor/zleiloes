from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import random,time

class BrowserManager:
    def __init__(self, user_agents):
        self.user_agents = user_agents
        self.options = self._configure_options()
        self.driver = self._create_driver()

    def _configure_options(self):
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument(f'user-agent={random.choice(self.user_agents)}')
        return options

    def _create_driver(self):
        return webdriver.Chrome(options=self.options)
    
    def load_all_properties(self, url):
        """Carrega todas as propriedades clicando em 'Carregar mais'"""
        self.driver.get(url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.card-property'))
        )
        self._close_popups()

        last_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
        attempts = 0

        while attempts < 5:  # Número máximo de tentativas
            try:
                load_more = WebDriverWait(self.driver, 7).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Carregar mais")]'))
                )
                self.driver.execute_script("arguments[0].click();", load_more)
                WebDriverWait(self.driver, 15).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, 'div.card-property')) > last_count
                )
                last_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
                attempts = 0
            except Exception:
                attempts += 1
                time.sleep(2)

        return self.driver.page_source

    def close(self):
        if self.driver:
            self.driver.quit()