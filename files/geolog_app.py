# -*- coding: utf-8 -*-
"""
GeoLog — Plataforma de Telemetria Logística e Persistência Poliglota
=====================================================================
UNIPÊ | Tópicos Avançados em Banco de Dados / Arquitetura de Software
Desafio Integrador GeoLog

Arquitetura Poliglota:
- SQLite  -> dados TRANSACIONAIS/RELACIONAIS (motoristas, veículos)
- MongoDB -> dados de TELEMETRIA/GEOESPACIAIS (GPS, sensores) com índice 2dsphere

Como executar:
    pip install streamlit pymongo folium streamlit-folium plotly pandas
    streamlit run geolog_app.py

Pré-requisito: uma instância MongoDB acessível (local ou Atlas). Configure
a connection string na barra lateral da aplicação.
"""

import random
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import GEOSPHERE, MongoClient, errors as mongo_errors

try:
    import folium
    from streamlit_folium import st_folium
    FOLIUM_OK = True
except ImportError:
    FOLIUM_OK = False

# =====================================================================
# CONFIGURAÇÃO GERAL
# =====================================================================
st.set_page_config(page_title="GeoLog | LogiTech Express", page_icon="🚚", layout="wide")

SQLITE_PATH_DEFAULT = "logitech.db"
MONGO_URI_DEFAULT = "mongodb://localhost:27017"
MONGO_DB_DEFAULT = "geolog_db"
MONGO_COLLECTION = "telemetria"

# --- Seed de dados sugerido pelo enunciado -----------------------------
MOTORISTAS_SEED = [
    (1, "Carlos Andrade", "123456789", "Ativo"),
    (2, "Mariana Silva", "987654321", "Ativo"),
    (3, "Roberto Souza", "456789123", "Em Descanso"),
]

VEICULOS_SEED = [
    (101, "ABC-1A23", "Volvo FH 540", 1),
    (102, "XYZ-9876", "Scania R450", 2),
    (103, "KGB-4567", "Mercedes Actros", 3),
]

TELEMETRIA_SEED = [
    {
        "veiculo_id": 101,
        "location": {"type": "Point", "coordinates": [-34.873, -7.115]},  # João Pessoa Centro
        "temperatura": 4.2,
        "velocidade": 65,
        "timestamp": "2026-09-11T10:00:00Z",
    },
    {
        "veiculo_id": 102,
        "location": {"type": "Point", "coordinates": [-34.832, -7.121]},  # Cabo Branco
        "temperatura": -18.5,
        "velocidade": 85,
        "timestamp": "2026-09-11T10:05:00Z",
    },
    {
        "veiculo_id": 103,
        "location": {"type": "Point", "coordinates": [-34.950, -7.150]},  # Tibiri / BR-230
        "temperatura": 22.0,
        "velocidade": 0,
        "timestamp": "2026-09-11T09:45:00Z",
    },
]

PONTOS_APOIO = {
    "João Pessoa (Centro)": (-7.115, -34.873),
    "Cabo Branco": (-7.121, -34.832),
    "Tibiri / BR-230": (-7.150, -34.950),
    "Campina Grande (referência regional)": (-7.230, -35.881),
}


# =====================================================================
# MÓDULO 1 — CAMADA DE PERSISTÊNCIA POLIGLOTA & CARGA INICIAL (SEED)
# =====================================================================
@st.cache_resource(show_spinner=False)
def get_sqlite_conn(path: str):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@st.cache_resource(show_spinner=False)
def get_mongo_client(uri: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=4000)
    client.admin.command("ping")  # força a validação da conexão
    return client


def init_sqlite(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS motoristas (
            id INTEGER PRIMARY KEY,
            nome TEXT NOT NULL,
            cnh TEXT NOT NULL,
            status TEXT NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS veiculos (
            id INTEGER PRIMARY KEY,
            placa TEXT NOT NULL,
            modelo TEXT NOT NULL,
            motorista_id INTEGER,
            FOREIGN KEY (motorista_id) REFERENCES motoristas(id)
        );
    """)
    conn.commit()

    if conn.execute("SELECT COUNT(*) FROM motoristas").fetchone()[0] == 0:
        conn.executemany("INSERT INTO motoristas VALUES (?,?,?,?)", MOTORISTAS_SEED)
    if conn.execute("SELECT COUNT(*) FROM veiculos").fetchone()[0] == 0:
        conn.executemany("INSERT INTO veiculos VALUES (?,?,?,?)", VEICULOS_SEED)
    conn.commit()


def init_mongo(client: MongoClient, db_name: str):
    db = client[db_name]
    col = db[MONGO_COLLECTION]
    # Índice geoespacial 2dsphere — obrigatório para $near / $geoWithin / $geoNear
    col.create_index([("location", GEOSPHERE)])
    if col.count_documents({}) == 0:
        seed = [dict(d, timestamp=d["timestamp"]) for d in TELEMETRIA_SEED]
        col.insert_many(seed)
    return col


def reset_all(sqlite_conn, mongo_col):
    sqlite_conn.execute("DELETE FROM veiculos")
    sqlite_conn.execute("DELETE FROM motoristas")
    sqlite_conn.commit()
    mongo_col.delete_many({})
    init_sqlite(sqlite_conn)
    mongo_col.insert_many([dict(d) for d in TELEMETRIA_SEED])


# =====================================================================
# FUNÇÕES AUXILIARES DE CONSULTA
# =====================================================================
def fetch_motoristas_veiculos(conn) -> pd.DataFrame:
    query = """
        SELECT v.id AS veiculo_id, v.placa, v.modelo,
               m.id AS motorista_id, m.nome AS motorista, m.status
        FROM veiculos v
        JOIN motoristas m ON m.id = v.motorista_id
    """
    return pd.read_sql_query(query, conn)


def fetch_ultima_telemetria(col) -> pd.DataFrame:
    """Aggregation pipeline: último registro de telemetria por veículo."""
    pipeline = [
        {"$sort": {"veiculo_id": 1, "timestamp": -1}},
        {"$group": {
            "_id": "$veiculo_id",
            "temperatura": {"$first": "$temperatura"},
            "velocidade": {"$first": "$velocidade"},
            "location": {"$first": "$location"},
            "timestamp": {"$first": "$timestamp"},
        }},
    ]
    docs = list(col.aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["veiculo_id", "temperatura", "velocidade", "lat", "lng", "timestamp"])
    rows = []
    for d in docs:
        coords = d["location"]["coordinates"]
        rows.append({
            "veiculo_id": d["_id"],
            "temperatura": d["temperatura"],
            "velocidade": d["velocidade"],
            "lng": coords[0],
            "lat": coords[1],
            "timestamp": d["timestamp"],
        })
    return pd.DataFrame(rows)


def fetch_por_raio(col, lat: float, lng: float, raio_km: float) -> pd.DataFrame:
    """Consulta geoespacial via $geoNear: veículos a até X km do ponto de referência."""
    pipeline = [
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": [lng, lat]},
                "distanceField": "distancia_m",
                "maxDistance": raio_km * 1000,
                "spherical": True,
            }
        },
        {"$sort": {"veiculo_id": 1, "timestamp": -1}},
        {"$group": {
            "_id": "$veiculo_id",
            "temperatura": {"$first": "$temperatura"},
            "velocidade": {"$first": "$velocidade"},
            "location": {"$first": "$location"},
            "timestamp": {"$first": "$timestamp"},
            "distancia_m": {"$first": "$distancia_m"},
        }},
        {"$sort": {"distancia_m": 1}},
    ]
    docs = list(col.aggregate(pipeline))
    rows = []
    for d in docs:
        coords = d["location"]["coordinates"]
        rows.append({
            "veiculo_id": d["_id"],
            "temperatura": d["temperatura"],
            "velocidade": d["velocidade"],
            "lng": coords[0],
            "lat": coords[1],
            "timestamp": d["timestamp"],
            "distancia_km": round(d["distancia_m"] / 1000, 2),
        })
    return pd.DataFrame(rows)


def fetch_historico(col) -> pd.DataFrame:
    docs = list(col.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame(columns=["veiculo_id", "temperatura", "velocidade", "timestamp"])
    rows = []
    for d in docs:
        rows.append({
            "veiculo_id": d["veiculo_id"],
            "temperatura": d["temperatura"],
            "velocidade": d["velocidade"],
            "timestamp": d["timestamp"],
        })
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp")


# =====================================================================
# BÔNUS — SIMULADOR DE TELEMETRIA EM TEMPO REAL
# =====================================================================
def simular_movimentacao(col):
    ultimos = fetch_ultima_telemetria(col)
    novos_docs = []
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for _, row in ultimos.iterrows():
        novo_lat = row["lat"] + random.uniform(-0.004, 0.004)
        novo_lng = row["lng"] + random.uniform(-0.004, 0.004)
        nova_temp = round(row["temperatura"] + random.uniform(-0.8, 0.8), 1)
        nova_vel = max(0, int(row["velocidade"] + random.randint(-15, 15)))
        novos_docs.append({
            "veiculo_id": int(row["veiculo_id"]),
            "location": {"type": "Point", "coordinates": [novo_lng, novo_lat]},
            "temperatura": nova_temp,
            "velocidade": nova_vel,
            "timestamp": agora,
        })
    if novos_docs:
        col.insert_many(novos_docs)
    return len(novos_docs)


# =====================================================================
# BARRA LATERAL — CONEXÕES
# =====================================================================
st.sidebar.title("🚚 GeoLog")
st.sidebar.caption("LogiTech Express — Persistência Poliglota")

with st.sidebar.expander("⚙️ Configuração das conexões", expanded=False):
    sqlite_path = st.text_input("Caminho do SQLite", value=SQLITE_PATH_DEFAULT)
    mongo_uri = st.text_input("MongoDB URI", value=MONGO_URI_DEFAULT)
    mongo_db_name = st.text_input("Banco Mongo", value=MONGO_DB_DEFAULT)

sqlite_conn = get_sqlite_conn(sqlite_path)
init_sqlite(sqlite_conn)

mongo_ok = True
try:
    mongo_client = get_mongo_client(mongo_uri)
    mongo_col = init_mongo(mongo_client, mongo_db_name)
except mongo_errors.PyMongoError as e:
    mongo_ok = False
    st.sidebar.error(f"❌ Falha ao conectar no MongoDB: {e}")

if st.sidebar.button("🔄 Resetar & recarregar seed"):
    if mongo_ok:
        reset_all(sqlite_conn, mongo_col)
        st.sidebar.success("Dados reiniciados.")
        st.rerun()
    else:
        st.sidebar.warning("Conecte o MongoDB antes de resetar.")

pagina = st.sidebar.radio(
    "Módulos",
    [
        "🏠 Visão Geral & Seed",
        "📍 Geoprocessamento (Raio)",
        "🔗 Visão Unificada (Join)",
        "📊 Dashboard Analítico",
        "🎲 Simulador em Tempo Real (Bônus)",
    ],
)

if not mongo_ok:
    st.error(
        "Não foi possível conectar ao MongoDB com a URI informada. "
        "Ajuste a connection string na barra lateral e tente novamente."
    )
    st.stop()

# =====================================================================
# PÁGINA 1 — VISÃO GERAL & SEED
# =====================================================================
if pagina == "🏠 Visão Geral & Seed":
    st.title("🏠 Visão Geral da Persistência Poliglota")
    st.markdown(
        """
        **Arquitetura:**
        - `SQLite (logitech.db)` → dados transacionais/relacionais: `motoristas`, `veiculos`.
        - `MongoDB (geolog_db.telemetria)` → dados geoespaciais/IoT, com índice **2dsphere**
          criado automaticamente na inicialização.
        """
    )
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🗄️ SQLite — Motoristas")
        st.dataframe(pd.read_sql_query("SELECT * FROM motoristas", sqlite_conn), use_container_width=True)
        st.subheader("🗄️ SQLite — Veículos")
        st.dataframe(pd.read_sql_query("SELECT * FROM veiculos", sqlite_conn), use_container_width=True)
    with col2:
        st.subheader("🍃 MongoDB — Telemetria (bruto)")
        st.dataframe(fetch_historico(mongo_col), use_container_width=True)
        idx_info = mongo_col.index_information()
        st.caption(f"Índices ativos na coleção `telemetria`: {list(idx_info.keys())}")

# =====================================================================
# PÁGINA 2 — GEOPROCESSAMENTO E BUSCA POR RAIO
# =====================================================================
elif pagina == "📍 Geoprocessamento (Raio)":
    st.title("📍 Geoprocessamento — Busca por Raio ($geoNear)")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        ponto_nome = st.selectbox("Ponto de apoio de referência", list(PONTOS_APOIO.keys()))
    with c2:
        raio_km = st.slider("Raio de busca (km)", min_value=1, max_value=50, value=10)
    with c3:
        st.write("")
        st.write("")
        buscar = st.button("🔎 Buscar veículos no raio", use_container_width=True)

    ref_lat, ref_lng = PONTOS_APOIO[ponto_nome]

    resultado = fetch_por_raio(mongo_col, ref_lat, ref_lng, raio_km)

    st.markdown(f"**{len(resultado)}** veículo(s) encontrado(s) em até **{raio_km} km** de *{ponto_nome}*.")
    st.dataframe(resultado, use_container_width=True)

    if FOLIUM_OK:
        m = folium.Map(location=[ref_lat, ref_lng], zoom_start=11, tiles="OpenStreetMap")
        folium.Marker(
            [ref_lat, ref_lng], tooltip="Ponto de Apoio",
            icon=folium.Icon(color="blue", icon="home"),
        ).add_to(m)
        folium.Circle(
            [ref_lat, ref_lng], radius=raio_km * 1000,
            color="blue", fill=True, fill_opacity=0.08,
        ).add_to(m)
        for _, row in resultado.iterrows():
            cor = "red" if row["velocidade"] > 80 else "green"
            folium.Marker(
                [row["lat"], row["lng"]],
                tooltip=f"Veículo {int(row['veiculo_id'])} • {row['distancia_km']} km",
                popup=(f"Veículo {int(row['veiculo_id'])}<br>"
                       f"Temp: {row['temperatura']}°C<br>Vel: {row['velocidade']} km/h"),
                icon=folium.Icon(color=cor, icon="truck", prefix="fa"),
            ).add_to(m)
        st_folium(m, width=None, height=480, key="mapa_raio")
    else:
        st.warning("Instale `folium` e `streamlit-folium` para visualizar o mapa interativo.")

# =====================================================================
# PÁGINA 3 — VISÃO UNIFICADA (JOIN POLIGLOTA EM MEMÓRIA)
# =====================================================================
elif pagina == "🔗 Visão Unificada (Join)":
    st.title("🔗 Visão Unificada — Join Poliglota em Memória")
    st.caption("SQLite (motoristas + veículos) ⋈ MongoDB (última telemetria) — combinados em memória com pandas.")

    df_sql = fetch_motoristas_veiculos(sqlite_conn)
    df_mongo = fetch_ultima_telemetria(mongo_col)

    df_join = df_sql.merge(df_mongo, on="veiculo_id", how="left")
    df_final = df_join.rename(columns={
        "motorista": "Nome do Motorista",
        "placa": "Placa",
        "temperatura": "Última Temperatura (°C)",
        "velocidade": "Velocidade (km/h)",
    })
    df_final["Coordenadas Atualizadas"] = df_final.apply(
        lambda r: f"({r['lat']:.4f}, {r['lng']:.4f})" if pd.notna(r.get("lat")) else "—", axis=1
    )

    st.dataframe(
        df_final[[
            "Nome do Motorista", "Placa", "Última Temperatura (°C)",
            "Velocidade (km/h)", "Coordenadas Atualizadas",
        ]],
        use_container_width=True,
    )

# =====================================================================
# PÁGINA 4 — DASHBOARD ANALÍTICO
# =====================================================================
elif pagina == "📊 Dashboard Analítico":
    st.title("📊 Dashboard Analítico")

    df_motoristas = pd.read_sql_query("SELECT * FROM motoristas", sqlite_conn)
    df_ultima = fetch_ultima_telemetria(mongo_col)
    df_hist = fetch_historico(mongo_col)

    k1, k2, k3 = st.columns(3)
    k1.metric("🚛 Frota ativa (com telemetria)", int(df_ultima["veiculo_id"].nunique()))
    media_temp = df_ultima["temperatura"].mean() if not df_ultima.empty else 0
    k2.metric("🌡️ Temperatura média da carga", f"{media_temp:.1f} °C")
    alertas = int((df_ultima["velocidade"] > 80).sum()) if not df_ultima.empty else 0
    k3.metric("⚠️ Alertas de velocidade (>80 km/h)", alertas)

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Histórico de temperatura por veículo")
        if not df_hist.empty:
            fig_temp = px.line(
                df_hist, x="timestamp", y="temperatura", color="veiculo_id",
                markers=True, labels={"veiculo_id": "Veículo"},
            )
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.info("Sem histórico de telemetria ainda.")
    with c2:
        st.subheader("Distribuição do status dos motoristas")
        status_counts = df_motoristas["status"].value_counts().reset_index()
        status_counts.columns = ["status", "quantidade"]
        fig_status = px.pie(status_counts, names="status", values="quantidade", hole=0.4)
        st.plotly_chart(fig_status, use_container_width=True)

# =====================================================================
# PÁGINA 5 — SIMULADOR EM TEMPO REAL (BÔNUS)
# =====================================================================
elif pagina == "🎲 Simulador em Tempo Real (Bônus)":
    st.title("🎲 Simulador de Telemetria em Tempo Real")
    st.caption(
        "Gera novos pontos GPS com pequena variação aleatória (lat/lng, temperatura e "
        "velocidade) para cada veículo e insere no MongoDB, atualizando mapa e dashboard."
    )

    if "sim_contagem" not in st.session_state:
        st.session_state.sim_contagem = 0

    if st.button("🚀 Simular Movimentação", type="primary"):
        n = simular_movimentacao(mongo_col)
        st.session_state.sim_contagem += 1
        st.success(f"{n} novo(s) registro(s) de telemetria inseridos no MongoDB.")

    st.metric("Rodadas de simulação executadas nesta sessão", st.session_state.sim_contagem)

    df_ultima = fetch_ultima_telemetria(mongo_col)
    st.subheader("Posições mais recentes (pós-simulação)")
    st.dataframe(df_ultima, use_container_width=True)

    if FOLIUM_OK and not df_ultima.empty:
        centro_lat = df_ultima["lat"].mean()
        centro_lng = df_ultima["lng"].mean()
        m = folium.Map(location=[centro_lat, centro_lng], zoom_start=11)
        for _, row in df_ultima.iterrows():
            cor = "red" if row["velocidade"] > 80 else "green"
            folium.Marker(
                [row["lat"], row["lng"]],
                tooltip=f"Veículo {int(row['veiculo_id'])}",
                popup=f"Temp: {row['temperatura']}°C<br>Vel: {row['velocidade']} km/h",
                icon=folium.Icon(color=cor, icon="truck", prefix="fa"),
            ).add_to(m)
        st_folium(m, width=None, height=480, key="mapa_simulador")

    st.caption(
        "Dica: clique novamente em **Simular Movimentação** para gerar mais pontos "
        "e ver o histórico de temperatura crescer no Dashboard Analítico."
    )
