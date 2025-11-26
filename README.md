📊 Projeto Big Data — Scraping e DashboardEste repositório contém um projeto de Big Data e Engenharia de Dados desenvolvido para fins acadêmicos. O objetivo é criar um pipeline automatizado que realiza a extração (scraping), transformação (limpeza) e visualização de dados de livros.O sistema acessa o site Books to Scrape, coleta informações dos primeiros livros listados e apresenta análises em um dashboard interativo.🛠️ Tech StackLinguagem: Python 3Visualização: StreamlitManipulação de Dados: PandasWeb Scraping: Selenium + WebDriver Manager (Modo Headless)📂 Estrutura do Projetoprojeto_bigdata/
├─ .venv/                 # Ambiente virtual (dependências instaladas)
├─ app.py                 # Aplicação Principal: Coleta, Limpeza e Dashboard
├─ scrape_test.py         # Script utilitário para testar o scraping isoladamente
├─ requirements.txt       # Lista de dependências do projeto
└─ README.md              # Documentação do projeto
Descrição dos Arquivosapp.py: O núcleo do projeto. Contém a lógica do Streamlit e o botão "Iniciar Coleta". Ele gerencia o driver do Selenium, trata erros, limpa os dados (removendo símbolos de moeda) e gera os gráficos.scrape_test.py: Script auxiliar para validar a lógica de extração via terminal, sem a necessidade de subir a interface gráfica.requirements.txt: Arquivo para reprodução do ambiente (versões do Selenium, Pandas, etc.).🚀 Como Executar (macOS/Linux)Siga os passos abaixo para rodar a aplicação utilizando o terminal (zsh/bash).1. Ativar o Ambiente VirtualCertifique-se de estar na raiz do projeto ou ajuste o caminho conforme necessário.# Exemplo usando caminho absoluto (ajuste para o seu usuário se necessário)
source /Users/willianrodriguespiments/Desktop/projeto_bigdata/.venv/bin/activate

# OU, se estiver na pasta do projeto:
source .venv/bin/activate
2. Rodar o DashboardInicie o servidor do Streamlit:streamlit run app.py
O navegador abrirá automaticamente em http://localhost:8501.3. Testes Rápidos (Opcional)Para verificar se as bibliotecas estão instaladas corretamente ou testar o scraping sem interface:# Teste de importação
python -c "import streamlit, pandas, selenium, webdriver_manager; print('✅ Imports OK')"

# Teste do script de scraping
python scrape_test.py
⚙️ Funcionamento TécnicoInicialização: O usuário clica em "Iniciar Coleta" no dashboard.Extração (Scraping): O webdriver_manager instala/atualiza o driver do Chrome. O Selenium abre o navegador em modo --headless (sem interface gráfica) e extrai o Título e Preço dos livros.Transformação (ETL): O Pandas recebe os dados brutos. A coluna de preço é limpa (remoção do símbolo £) e convertida para numérico (float).Visualização: O Streamlit exibe:Tabela de dados (DataFrame).Métrica de preço médio.Gráfico de barras comparativo.Resiliência (Fallback): Se o Selenium falhar (por falta do Chrome ou incompatibilidade de driver), o sistema captura a exceção e utiliza um Mock Data (dados fictícios) para garantir que a apresentação não seja interrompida.⚠️ Troubleshooting (Problemas Comuns)Exec Format Error / Incompatibilidade de DriverSe ocorrer erro indicando incompatibilidade entre arquiteturas (ARM vs x86) ou versão do Chrome:Solução Automática (App): O app passará a usar dados de teste (mock) automaticamente.Correção Manual: Limpe o cache do gerenciador de drivers para forçar o download da versão correta:rm -rf ~/.wdm
