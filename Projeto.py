# -*- coding: utf-8 -*-

"""
Notebook Técnico: Análise de Mobilidade Urbana em São Paulo com PySpark

Este notebook implementa um pipeline de dados completo para analisar
padrões de mobilidade urbana, cobrindo as seguintes etapas:
1.  ETL (Extração, Transformação e Carga) de dados em batch.
2.  Análise exploratória de dados utilizando consultas SQL.
3.  Preparação de dados para visualização.
4.  Implementação de um exemplo de Structured Streaming para análise em tempo real.
"""

# -----------------------------------------------------------------------------
# 1. SETUP E INICIALIZAÇÃO DO SPARK
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, dayofweek, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Inicializa a Spark Session
spark = SparkSession.builder \
    .appName("MobilidadeUrbanaSP") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("Spark Session iniciada com sucesso!")

# -----------------------------------------------------------------------------
# 2. ETL (EXTRACT, TRANSFORM, LOAD) - PROCESSAMENTO BATCH
# -----------------------------------------------------------------------------

# --- EXTRACTION (Extração) ---
# Em um cenário real, leríamos dados de um Data Lake (ex: S3, HDFS) em formatos
# como Parquet, CSV, etc.
# Ex: df = spark.read.parquet("s3a://dados-mobilidade/sp/raw/")
#
# Para este exemplo, vamos criar um DataFrame simulado para representar os dados.
dados_simulados = [
    (1, 1678886400, -23.5505, -46.6333, -23.5614, -46.6563, "onibus", "V001"),
    (2, 1678887000, -23.5614, -46.6563, -23.5880, -46.6358, "metro", "L1-AZUL"),
    (3, 1678887600, -23.5880, -46.6358, -23.5505, -46.6333, "app", "A002"),
    (4, 1678890000, -23.5505, -46.6333, -23.5489, -46.6388, "onibus", "V002"),
    (5, 1678891800, -23.6033, -46.6669, -23.5880, -46.6358, "onibus", "V001"),
    (6, 1678893600, -23.5489, -46.6388, -23.5505, -46.6333, "app", "A003"),
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
print("\nSchema dos dados brutos:")
df_raw.printSchema()
print("\nAmostra dos dados brutos:")
df_raw.show()


# --- TRANSFORMATION (Transformação) ---
# Limpeza, enriquecimento e preparação dos dados para análise.

df_transformed = df_raw \
    .withColumn("timestamp", from_unixtime(col("timestamp_unix")).cast(TimestampType())) \
    .withColumn("hora_do_dia", hour(col("timestamp"))) \
    .withColumn("dia_da_semana", dayofweek(col("timestamp"))) \
    .withColumn("periodo_dia",
        when((col("hora_do_dia") >= 6) & (col("hora_do_dia") < 12), "Manhã")
        .when((col("hora_do_dia") >= 12) & (col("hora_do_dia") < 18), "Tarde")
        .when((col("hora_do_dia") >= 18) & (col("hora_do_dia") < 24), "Noite")
        .otherwise("Madrugada")
    ) \
    .drop("timestamp_unix") # Coluna original não é mais necessária

# Tratamento de valores nulos (se houvesse)
df_cleaned = df_transformed.na.drop()

print("\nSchema dos dados transformados:")
df_cleaned.printSchema()
print("\nAmostra dos dados transformados:")
df_cleaned.show()


# --- LOAD (Carga) ---
# Salvando os dados transformados em um formato otimizado (Parquet)
# para consultas futuras.
# Em um ambiente real, isso seria salvo em uma camada "trusted" ou "refined" do Data Lake.

# df_cleaned.write.mode("overwrite").parquet("/path/to/datalake/trusted/mobilidade_sp")
print("\nETL em batch concluído. Dados prontos para análise.")


# -----------------------------------------------------------------------------
# 3. ANÁLISE EXPLORATÓRIA COM SQL
# -----------------------------------------------------------------------------

# Para usar SQL, criamos uma view temporária a partir do DataFrame.
df_cleaned.createOrReplaceTempView("mobilidade_sp")

print("\n--- Iniciando Análises com Spark SQL ---")

# Consulta 1: Contagem de viagens por tipo de transporte
print("\n[Consulta 1] Contagem de viagens por tipo de transporte:")
spark.sql("""
    SELECT 
        tipo_transporte, 
        COUNT(*) as total_viagens
    FROM mobilidade_sp
    GROUP BY tipo_transporte
    ORDER BY total_viagens DESC
""").show()

# Consulta 2: Horários de pico (maior volume de viagens por hora)
print("\n[Consulta 2] Viagens por hora do dia (Horários de Pico):")
spark.sql("""
    SELECT 
        hora_do_dia, 
        COUNT(*) as total_viagens
    FROM mobilidade_sp
    GROUP BY hora_do_dia
    ORDER BY hora_do_dia
""").show()

# Consulta 3: Viagens por período do dia
print("\n[Consulta 3] Viagens por período do dia:")
spark.sql("""
    SELECT 
        periodo_dia, 
        COUNT(*) as total_viagens
    FROM mobilidade_sp
    GROUP BY periodo_dia
    ORDER BY 
        CASE 
            WHEN periodo_dia = 'Manhã' THEN 1
            WHEN periodo_dia = 'Tarde' THEN 2
            WHEN periodo_dia = 'Noite' THEN 3
            ELSE 4
        END
""").show()

# -----------------------------------------------------------------------------
# 4. PREPARAÇÃO PARA VISUALIZAÇÃO DE DADOS
# -----------------------------------------------------------------------------
# Para criar gráficos em bibliotecas como Matplotlib ou Plotly, geralmente
# coletamos os resultados agregados para o driver como um DataFrame Pandas.
# CUIDADO: Use .toPandas() apenas em dados já agregados e de tamanho gerenciável.

print("\nColetando dados agregados para visualização...")
pandas_df_transporte = spark.sql("SELECT tipo_transporte, COUNT(*) as total FROM mobilidade_sp GROUP BY tipo_transporte").toPandas()
print("Dados por tipo de transporte (Pandas DF):\n", pandas_df_transporte)

pandas_df_hora = spark.sql("SELECT hora_do_dia, COUNT(*) as total FROM mobilidade_sp GROUP BY hora_do_dia ORDER BY hora_do_dia").toPandas()
print("\nDados por hora do dia (Pandas DF):\n", pandas_df_hora)

# Exemplo de como plotar (o código abaixo seria executado em um ambiente com bibliotecas de plotagem)
# import matplotlib.pyplot as plt
#
# plt.figure(figsize=(10, 6))
# plt.bar(pandas_df_transporte['tipo_transporte'], pandas_df_transporte['total'])
# plt.title('Contagem de Viagens por Tipo de Transporte')
# plt.xlabel('Tipo de Transporte')
# plt.ylabel('Total de Viagens')
# plt.show()


# -----------------------------------------------------------------------------
# 5. STRUCTURED STREAMING
# -----------------------------------------------------------------------------
# Simulando um fluxo de dados em tempo real. Em um cenário real, a fonte
# poderia ser Kafka, Kinesis, etc. Aqui, usaremos uma fonte de socket para
# demonstração.

# Para testar:
# 1. Abra um terminal.
# 2. Execute o comando: nc -lk 9999
# 3. No terminal do 'nc', cole dados no formato JSON, um por linha:
#    {"timestamp": 1678900000, "lat": -23.55, "lon": -46.63, "tipo_transporte": "onibus"}
#    {"timestamp": 1678900010, "lat": -23.56, "lon": -46.65, "tipo_transporte": "metro"}

print("\n--- Configurando Structured Streaming ---")
# Define o schema para os dados de entrada do stream
stream_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("tipo_transporte", StringType(), True),
])

# Lendo de um socket (para fins de demonstração)
# Em produção, usaríamos: spark.readStream.format("kafka")...
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse do JSON de entrada
from pyspark.sql.functions import from_json, col, window

df_stream_raw = lines.select(from_json(col("value"), stream_schema).alias("data")).select("data.*")

# Transformação: Contagem de eventos por tipo de transporte em janelas de 1 minuto
windowed_counts = df_stream_raw \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType())) \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("tipo_transporte")
    ).count().orderBy("window")

# Saída para o console
# Em produção, a saída (sink) poderia ser um dashboard, outro tópico Kafka, etc.
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("\nStream iniciado. Aguardando dados na porta 9999.")
print("Para parar, use query.stop() ou interrompa o processo.")

# query.awaitTermination() # Descomente para manter o script rodando indefinidamente

# -----------------------------------------------------------------------------
# 6. FINALIZAÇÃO
# -----------------------------------------------------------------------------
# Para o streaming não bloquear o fim do script de exemplo, vamos pará-lo após um tempo.
import time
time.sleep(60) # Deixa o stream rodar por 60 segundos
query.stop()
spark.stop()
print("\nStream e Spark Session finalizados.")
