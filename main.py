from fastapi import FastAPI

app = FastAPI()

# Raíz principal que muestra un mensaje
@app.get("/")
def raiz():
    return {"mensaje": "¡Bienvenido a la API principal!"}

# Función para leer 2 datos desde la URL
@app.get("/leer_datos")
def leer_datos(dato1: str, dato2: int):
    return {"mensaje": f"Recibimos dato1={dato1} y dato2={dato2}"}