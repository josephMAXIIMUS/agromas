from fastapi import FastAPI, HTTPException
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.types import String
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
load_dotenv() 
app = FastAPI()
apikey=os.getenv("AGROMAS_API_KEY")


def funcion_connect():
    server = os.getenv("DB_SERVER")
    driver = os.getenv("DB_DRIVER")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database_destino = os.getenv("DB_NAME")

    params_destino = quote_plus(
        f"DRIVER={driver};SERVER={server};DATABASE={database_destino};UID={username};PWD={password};TrustServerCertificate=yes"
    )
    engine_nueva = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params_destino}",
        fast_executemany=True
    )
    return engine_nueva

@app.get("/")
def raiz():
    return {apikey}




@app.get("/cargar_data")
def leer_datos(fecha: str, formato: int, nombre_tabla_db: str):
    from dotenv import load_dotenv
    try:
       
     
        # 1️⃣ Obtener datos de la API externa
        url = "https://agromas.safcoperu.pe:9443/packing/web/agrodigital/datos-registrados"
        
        params = {"fecha": fecha, "id_formato": formato}
        apikey=os.getenv("AGROMAS_API_KEY")
        headers = {"X-Agromas-Apikey": apikey}
        response = requests.get(url, params=params, headers=headers)
        data = response.json()

        registros = data["data"]["registros"]
        criterios = data["data"]["criterios"]

        datos_leidos = pd.DataFrame(registros, columns=criterios)
        numero_de_filas = len(datos_leidos)
    
        dtype_sql = {}
        for col in datos_leidos.columns:
            if datos_leidos[col].dtype == 'object':
                        # Texto (object) → VARCHAR(100)
                dtype_sql[col] = String(50)


        df_head = datos_leidos.head(0)
        nuevo = funcion_connect()
        # print(list(df_head.columns))
        # print(list(datos_leidos.columns))
        
        
        df_head.to_sql(nombre_tabla_db, con=nuevo, schema='AGROMAS', if_exists='append',dtype=dtype_sql, index=False,method='multi')


        engine_original = funcion_connect()
        consulta = (
            f"SELECT max(fecha) AS fecha_registrada "
            f"FROM AGROMAS.{nombre_tabla_db} "
            f"WHERE FECHA = '{fecha}'"
        )
        fecha_db = pd.read_sql(consulta, engine_original)
    
        
        fecha_registro= fecha_db['fecha_registrada'].iloc[0]
        fecha_registro_1=str(fecha_registro)
        var_fecha_1 = str(fecha)



        if fecha_registro_1 == var_fecha_1  :
            print("aqui")
            return("fecha ya registrada")
            #return None
        elif numero_de_filas == 0:
            return("fecha no encontrada")
        elif(fecha_registro_1 != var_fecha_1 ):
            try:
                engine_nueva = funcion_connect()

            # df_para_insertar.to_sql(nombre_tabla_db, con=engine_nueva, schema='AGROMAS', if_exists='replace', index=False)
                datos_leidos.to_sql(nombre_tabla_db, con=engine_nueva, schema='AGROMAS', if_exists='append',dtype=dtype_sql, index=False)
                        #append
                        #replace
                #print(datos_leidos)
                return("datos ingresados")          
            except Exception as e:
                    print("❌ Error al obtener o insertar datos:", e)
        return fecha,formato,nombre_tabla_db, a

    except Exception as e:
        # Devuelve error como mensaje
        raise HTTPException(status_code=500, detail=f"❌ Error al obtener o insertar datos: {e}")

