# Imagen base
FROM python:3.9-bullseye

# Se copia el script y los requerimentos
COPY tracker.py tracker.py
COPY requirements.txt requirements.txt

# Se instalan los requerimentos
RUN pip3 install -r requirements.txt

# El puerto por defecto es el 8050, por lo que se expone
EXPOSE 8050 8080

# Se ejecuta el script
CMD python3 tracker.py