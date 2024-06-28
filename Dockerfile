# Utiliza una imagen base oficial de Python
FROM python:3.12-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de requisitos y los instala
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el contenido de la carpeta actual al contenedor
COPY . .

# Expone el puerto en el que la aplicación escuchará
EXPOSE 4142

# Comando para ejecutar la aplicación
CMD ["python", "src/main.py"]


