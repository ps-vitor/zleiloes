from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import random
from bs4 import BeautifulSoup
import traceback

# Configuração avançada para evitar detecção
options = webdriver.ChromeOptions()
options.add_argument('--headless=new')  # Novo modo headless mais discreto
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)

# Configurações adicionais para parecer mais humano
options.add_argument('--disable-infobars')
options.add_argument('--disable-notifications')
options.add_argument('--disable-popup-blocking')
options.add_argument('--disable-save-password-bubble')

driver = webdriver.Chrome(options=options)

# Script para ocultar o WebDriver
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """
    Object.defineProperty(navigator, 'webdriver', {
      get: () => undefined
    });
    window.chrome = {
      runtime: {},
    };
    Object.defineProperty(navigator, 'plugins', {
      get: () => [1, 2, 3],
    });
    Object.defineProperty(navigator, 'languages', {
      get: () => ['pt-BR', 'pt', 'en-US', 'en'],
    });
  """
})

def human_like_delay():
    """Adiciona um atraso aleatório para parecer humano"""
    time.sleep(random.uniform(0.5, 2.5))

def scroll_to_element(element):
    """Rola suavemente até o elemento"""
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
    human_like_delay()

def try_click(selector, by=By.CSS_SELECTOR, max_attempts=3):
    """Tenta clicar em um elemento com múltiplas tentativas"""
    for _ in range(max_attempts):
        try:
            element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((by, selector)))
            scroll_to_element(element)
            element.click()
            human_like_delay()
            return True
        except Exception as e:
            print(f"Tentativa de clique falhou: {str(e)}")
            time.sleep(1)
    return False

url = 'https://www.portalzuk.com.br/leilao-de-imoveis'

try:
    print("Acessando a página de forma discreta...")
    driver.get(url)
    human_like_delay()
    
    # Verificar se houve redirecionamento
    if "leilao-de-imoveis" not in driver.current_url:
        print("Redirecionado para outra página. Ajustando...")
        driver.get(url)
        human_like_delay()
    
    # Tentar fechar possíveis pop-ups
    try:
        if try_click('button.close', By.CSS_SELECTOR) or \
           try_click('div.modal-close', By.CSS_SELECTOR) or \
           try_click('//button[contains(text(), "Fechar")]', By.XPATH):
            print("Pop-up fechado.")
    except:
        pass
    
    # Verificar se a página carregou corretamente
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.property-item, div.listing-container'))
        )
        print("Página carregada com sucesso.")
    except TimeoutException:
        # Tentar alternativa caso o conteúdo principal não carregue
        print("Tentando abordagem alternativa...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        human_like_delay()
        driver.execute_script("window.scrollTo(0, 0)")
        human_like_delay()
    
    # Carregar todos os imóveis
    last_count = 0
    current_count = len(driver.find_elements(By.CSS_SELECTOR, 'div.property-item'))
    print(f"Imóveis iniciais encontrados: {current_count}")
    
    while current_count < 989:
        # Tentar clicar no botão "Carregar mais"
        if not try_click('//button[contains(., "Carregar mais")]', By.XPATH):
            print("Botão 'Carregar mais' não encontrado ou não clicável.")
            break
        
        # Esperar novos itens carregarem
        try:
            WebDriverWait(driver, 15).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, 'div.property-item')) > current_count)
            
            new_count = len(driver.find_elements(By.CSS_SELECTOR, 'div.property-item'))
            if new_count == current_count:
                print("Nenhum novo imóvel carregado. Parando...")
                break
                
            current_count = new_count
            print(f"Total de imóveis carregados: {current_count}")
            
            # Adicionar rolagem aleatória para parecer humano
            if random.random() > 0.7:
                driver.execute_script(f"window.scrollBy(0, {random.randint(200, 800)})")
                human_like_delay()
                
        except TimeoutException:
            print("Timeout ao esperar novos imóveis.")
            break
    
    # Extrair dados usando BeautifulSoup para maior resiliência
    print("Extraindo dados dos imóveis...")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    property_cards = soup.select('div.property-item')
    imoveis = []
    
    for card in property_cards:
        try:
            imovel = {
                'titulo': card.select_one('h2').get_text(strip=True) if card.select_one('h2') else None,
                'endereco': card.select_one('p.address').get_text(strip=True) if card.select_one('p.address') else None,
                'preco': card.select_one('span.price').get_text(strip=True) if card.select_one('span.price') else None,
                'link': card.select_one('a')['href'] if card.select_one('a') else None
            }
            imoveis.append(imovel)
        except Exception as e:
            print(f"Erro ao processar um imóvel: {str(e)}")
            continue
    
    # Salvar os dados
    import json
    with open('imoveis_portal_zuk.json', 'w', encoding='utf-8') as f:
        json.dump(imoveis, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Dados de {len(imoveis)} imóveis salvos com sucesso!")

except Exception as e:
    print(f"❌ Erro durante a execução: {str(e)}")
    traceback.print_exc()
    driver.save_screenshot('error_final.png')
    print("📸 Screenshot salvo como 'error_final.png'")

finally:
    driver.quit()