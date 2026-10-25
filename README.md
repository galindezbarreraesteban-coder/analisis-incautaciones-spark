# Análisis de incautaciones de base de coca en Colombia mediante Apache Spark

## 1. Descripción del proyecto
Este proyecto desarrolla una solución de procesamiento Batch utilizando Python y Apache Spark para analizar información sobre incautaciones de base de coca en Colombia.
El conjunto de datos se obtiene del portal oficial de Datos Abiertos Colombia y contiene registros históricos que permiten estudiar la distribución geográfica y temporal de las incautaciones registradas.
**Fuente de datos:** https://www.datos.gov.co/en/Seguridad-y-Defensa/INCAUTACIONES-DE-BASE-DE-COCA/nxbk-nikm/about_data

## 2. Descripción de la solución
La solución utiliza Apache Spark para procesar el conjunto de datos almacenado en formato CSV. Mediante Python y PySpark, se realizan operaciones de preparación, transformación y análisis exploratorio de los datos.
El procesamiento permite examinar los registros por año y departamento, así como analizar las cantidades registradas según los criterios implementados en el programa.
Los resultados se representan mediante gráficos generados con Matplotlib, facilitando la interpretación de los datos y la identificación de patrones temporales y geográficos.
### Principales funcionalidades
- Carga del conjunto de datos desde un archivo CSV.
- Limpieza y transformación de los registros.
- Análisis exploratorio de los datos mediante Apache Spark.
- Análisis de registros por año.
- Análisis de cantidades registradas por año.
- Identificación de los diez departamentos principales según el criterio utilizado en el programa.
- Generación y almacenamiento de gráficos para visualizar los resultados.

## 3. Tecnologías utilizadas
- **Python:** lenguaje de programación utilizado para desarrollar la aplicación.
- **Apache Spark:** motor de procesamiento distribuido de datos.
- **PySpark:** interfaz de Python para trabajar con Apache Spark.
- **Matplotlib:** biblioteca utilizada para generar las visualizaciones.
- **Ubuntu:** entorno de ejecución del programa.

## 4. Requisitos previos
Para ejecutar el programa se requiere:
- Python 3.
- Apache Spark instalado y configurado.
- Java compatible con la versión de Spark instalada.
- PySpark disponible en el entorno de ejecución.
- Matplotlib instalado.
- El archivo CSV del conjunto de datos descargado desde la fuente original.

## 5. Organización del proyecto
El repositorio contiene el código fuente del procesamiento Batch:
```text
analisis-incautaciones-spark/
├── analisis_incautaciones.py
└── README.md
```
Los gráficos generados se guardan en el directorio `resultados_incautaciones`, según la configuración del programa.

## 6. Instrucciones para la ejecución
### Paso 1. Obtener el conjunto de datos
Descargar el archivo CSV desde el portal de Datos Abiertos Colombia:
https://www.datos.gov.co/en/Seguridad-y-Defensa/INCAUTACIONES-DE-BASE-DE-COCA/nxbk-nikm
Guardar el archivo en una ubicación accesible desde el entorno donde se ejecutará Spark.
### Paso 2. Configurar la ruta del archivo
Abrir el archivo `analisis_incautaciones.py` y comprobar que la variable que contiene la ruta del CSV coincida con la ubicación real del conjunto de datos.
La ruta utilizada en el entorno de desarrollo es:
```text
/home/vboxuser/INCAUTACIONES_DE_BASE_DE_COCA_20261024.csv
```
Si el archivo se encuentra en otra ubicación, se debe actualizar la ruta en el código.
### Paso 3. Instalar las dependencias
Verificar que Apache Spark, Java, PySpark y Matplotlib estén instalados y configurados.
Si Matplotlib no está instalado en el entorno de Python utilizado por Spark, puede instalarse mediante:
```bash
python3 -m pip install matplotlib
```
### Paso 4. Ejecutar el procesamiento Batch
Desde la terminal de Ubuntu, ejecutar:
```bash
/opt/spark/bin/spark-submit ~/analisis_incautaciones.py
```
Este comando inicia la aplicación mediante `spark-submit`, utilizando la instalación de Apache Spark ubicada en `/opt/spark`.
Si el archivo Python se encuentra en otro directorio, se debe reemplazar la ruta por la ubicación correspondiente.
### Paso 5. Consultar los resultados
Al finalizar el procesamiento, verificar los gráficos generados en el directorio:
```text
/home/vboxuser/resultados_incautaciones/
```
Los gráficos esperados son:
- `top_10_departamentos.png`
- `registros_por_anio.png`
- `kilogramos_por_anio.png`
Estos archivos permiten visualizar los resultados del análisis realizado sobre los registros históricos.

## 7. Resultados esperados
La ejecución del programa permite obtener resúmenes y visualizaciones del conjunto de datos de incautaciones de base de coca en Colombia.
Los resultados facilitan el análisis de la distribución geográfica de los registros y su comportamiento a lo largo del tiempo.
Las conclusiones deben formularse a partir de los resultados efectivamente obtenidos durante la ejecución del programa.

## 8. Consideraciones
- El archivo CSV debe estar disponible en la ruta configurada en el programa.
- La ruta de Spark puede variar según la instalación.
- Los gráficos dependen de los datos cargados y de las operaciones implementadas en el código.
- El análisis describe los registros disponibles en la fuente de datos y no debe interpretarse, por sí solo, como una medida completa de la actividad delictiva real.
