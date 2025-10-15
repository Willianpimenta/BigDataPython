# -*- coding: utf-8 -*-
"""
Script PySpark para Análise de Mobilidade Urbana e Geração de Dados para Dashboard.

Este script executa as seguintes etapas:
1.  Inicializa uma Spark Session.
2.  Simula a leitura de dados brutos de mobilidade.
3.  Executa um pipeline ETL para transformar e enriquecer os dados.
4.  Realiza agregações com Spark SQL para extrair insights.
5.  Coleta os resultados e os exporta para um arquivo JSON (`dashboard_data.json`),
    que servirá como fonte de dados para o painel web.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, dayofweek, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

def main():
    """
    Função principal que orquestra todo o processo de análise e geração de dados.
    """
    # -----------------------------------------------------------------------------
    # 1. SETUP E INICIALIZAÇÃO DO SPARK
    # -----------------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("GeradorDashboardMobilidade") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("Spark Session iniciada com sucesso!")

    # -----------------------------------------------------------------------------
    # 2. ETL (EXTRACT, TRANSFORM, LOAD)
    # -----------------------------------------------------------------------------
    
    # Para este exemplo, vamos usar um conjunto de dados simulado maior para
    # que os gráficos fiquem mais interessantes.
    dados_simulados = [
        # Manhã (Pico)
        (1, 1678878000, -23.55, -46.63, -23.56, -46.65, "onibus", "V001"), # 08:00
        (2, 1678878300, -23.56, -46.65, -23.58, -46.63, "metro", "L1-AZUL"),# 08:05
        (3, 1678878600, -23.58, -46.63, -23.55, -46.63, "app", "A002"),    # 08:10
        (4, 1678881600, -23.54, -46.64, -23.55, -46.63, "onibus", "V002"), # 09:00
        (5, 1678882200, -23.55, -46.63, -23.58, -46.63, "onibus", "V001"), # 09:10
        
        # Tarde
        (6, 1678893600, -23.57, -46.66, -23.55, -46.63, "app", "A003"),    # 12:00
        (7, 1678897200, -23.55, -46.63, -23.56, -46.65, "metro", "L4-AMARELA"), # 13:00
        (8, 1678900800, -23.56, -46.65, -23.58, -46.63, "onibus", "V003"), # 14:00

        # Tarde/Noite (Pico)
        (9, 1678912800, -23.58, -46.63, -23.55, -46.63, "onibus", "V001"), # 17:20
        (10, 1678913400, -23.54, -46.64, -23.58, -46.63, "app", "A004"),   # 17:30
        (11, 1678916400, -23.56, -46.65, -23.55, -46.63, "metro", "L1-AZUL"), # 18:20
        (12, 1678917000, -23.55, -46.63, -23.54, -46.64, "onibus", "V004"), # 18:30
        (13, 1678917300, -23.58, -46.63, -23.56, -46.65, "onibus", "V001"), # 18:35
    ]
    schema = StructType([
        StructField("id_viagem", LongType(), True),
        StructField("timestamp_unix", LongType(), True),
        StructField("lat_origem", DoubleType(), True),
        StructField("lon_origem", DoubleType(), True),
        StructField("lat_destino", DoubleType(), True),
        StructField("lon_destino", DoubleType(), True),
        StructField("tipo_transporte", StringType(), True),
        StructField("id_veiculo", StringType(), True),
    ])
    df_raw = spark.createDataFrame(data=dados_simulados, schema=schema)

    # Transformação
    df_cleaned = df_raw \
        .withColumn("timestamp", from_unixtime(col("timestamp_unix")).cast(TimestampType())) \
        .withColumn("hora_do_dia", hour(col("timestamp"))) \
        .drop("timestamp_unix")

    df_cleaned.createOrReplaceTempView("mobilidade_sp")
    print("ETL concluído e view 'mobilidade_sp' criada.")

    # -----------------------------------------------------------------------------
    # 3. ANÁLISE E PREPARAÇÃO DOS DADOS PARA O JSON
    # -----------------------------------------------------------------------------
    print("Iniciando agregações para o dashboard...")
    
    # Consulta 1: Contagem por tipo de transporte
    df_transporte = spark.sql("""
        SELECT tipo_transporte, COUNT(*) as total
        FROM mobilidade_sp
        GROUP BY tipo_transporte
    """)

    # Consulta 2: Contagem por hora do dia
    df_hora = spark.sql("""
        SELECT hora_do_dia, COUNT(*) as total
        FROM mobilidade_sp
        GROUP BY hora_do_dia
        ORDER BY hora_do_dia
    """)

    # Coletando os resultados para o driver
    # CUIDADO: Use .collect() apenas com dados agregados e pequenos.
    dados_transporte = [row.asDict() for row in df_transporte.collect()]
    dados_hora = [row.asDict() for row in df_hora.collect()]

    # -----------------------------------------------------------------------------
    # 4. CÁLCULO DE KPIs E ESTRUTURAÇÃO DO JSON FINAL
    # -----------------------------------------------------------------------------
    
    # Calcula o total de viagens
    total_viagens = sum(item['total'] for item in dados_transporte)
    
    # Encontra o principal modal
    principal_modal = max(dados_transporte, key=lambda x: x['total'])['tipo_transporte']

    # Monta a estrutura final do JSON
    dashboard_data = {
        "kpis": {
            "total_viagens": total_viagens,
            "principal_modal": principal_modal.capitalize(),
            "pico_manha": "08:00 - 09:00", # Mantido estático como no relatório
            "pico_tarde": "17:00 - 19:00"  # Mantido estático como no relatório
        },
        "graficos": {
            "distribuicao_modal": dados_transporte,
            "volume_por_hora": dados_hora
        }
    }
    
    # -----------------------------------------------------------------------------
    # 5. SALVANDO O ARQUIVO JSON
    # -----------------------------------------------------------------------------
    output_path = "dashboard_data.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=4)

    print(f"\nDados para o dashboard gerados com sucesso em '{output_path}'")

    # Finaliza a sessão Spark
    spark.stop()
    print("Spark Session finalizada.")


if __name__ == "__main__":
    main()
