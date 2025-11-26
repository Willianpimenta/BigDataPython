import streamlit as st
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

# Cofig pagina
st.title("Projeto Big Data - Análise de Livros")
st.write("Coleta simples dos 5 primeiros livros de books.toscrape.com")

def conectar_site():
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # roda sem abrir janela
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    except Exception:
        return None


def coletar_dados():
    driver = conectar_site()
    dados = []

    if driver:
        with st.spinner("Raspando dados..."):
            try:
                driver.get("http://books.toscrape.com/")
                livros = driver.find_elements(By.CSS_SELECTOR, "article.product_pod")
                for livro in livros[:5]:
                    titulo = livro.find_element(By.CSS_SELECTOR, "h3 a").get_attribute("title")
                    preco = livro.find_element(By.CSS_SELECTOR, ".price_color").text
                    dados.append({"Titulo": titulo, "Preco": preco})
            except Exception:
                st.error("Erro ao raspar com Selenium. Usando dados de exemplo.")
            finally:
                try:
                    driver.quit()
                except Exception:
                    pass

    if not dados:
        # Dados ficticios como fallback
        dados = [
            {"Titulo": "A Light in the Attic", "Preco": "£51.77"},
            {"Titulo": "Tipping the Velvet", "Preco": "£53.74"},
            {"Titulo": "Soumission", "Preco": "£50.10"},
            {"Titulo": "Sharp Objects", "Preco": "£47.82"},
            {"Titulo": "Sapiens", "Preco": "£54.23"}
        ]

    return pd.DataFrame(dados)


if st.button("Iniciar Coleta e Análise"):
    df = coletar_dados()

    # Limpeza de preços: remove '£' e converte para float 
    try:
        df["Preco_Num"] = (
            df["Preco"].astype(str).str.replace("£", "", regex=False).str.strip().astype(float)
        )
    except Exception:
        st.warning("Falha ao converter preços; preenchendo com zeros.")
        df["Preco_Num"] = 0.0

    st.subheader("Tabela de Livros")
    st.dataframe(df[["Titulo", "Preco", "Preco_Num"]])

    st.subheader("Preço por Livro")
    try:
        st.bar_chart(df, x="Titulo", y="Preco_Num")
    except Exception:
        st.info("Não foi possível gerar o gráfico.")

    # Estatística simples
    try:
        media = df["Preco_Num"].mean()
        st.metric("Preço Médio", f"£ {media:.2f}")
    except Exception:
        st.write("Preço médio indisponível")