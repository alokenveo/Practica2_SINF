# MapReduce Log Analysis

Este proyecto implementa una serie de tareas de análisis de registros usando Hadoop MapReduce. Las tareas incluyen cálculo de frecuencia de URLs, errores HTTP, visitas por IP única, frecuencia por hora y distribución por dispositivo.

## Tabla de Contenidos

- [Requisitos](#requisitos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Uso](#uso)
- [Clases Implementadas](#clases-implementadas)
- [Contribuciones](#contribuciones)

## Requisitos

- **Java 8 o superior**  
- **Apache Hadoop 3.x o superior**  
- **Sistema operativo compatible** con Hadoop  
- **Maven** (opcional, para compilar el proyecto si usas dependencias externas)

## Estructura del Proyecto
src/ ├── main/ │ └── java/ │ └── model/ │ └── MainMapReduceApp.java

## Uso

### Compilación
1. Clonar el repositorio:
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd <NOMBRE_DEL_REPOSITORIO>
2. Compilar el código:
   ```bash
   javac -cp $(hadoop classpath) -d . src/main/java/model/MainMapReduceApp.java
4. Crear el archivo.jar:
   ```bash
   jar -cvf mapreduce-log-analysis.jar -C . .

### Ejecución
1. Ejecutar el programa:
2. Analizar los resultados:  Cada tarea genera su salida en un subdirectorio dentro de <output_path>:
   - visitas_por_url/
   - errores_http/
   - visitas_por_ip_unica/
   - frecuencia_por_hora/
   - distribucion_por_dispositivo/

### Ejemplo de ejecución
   ```bash
   hadoop jar mapreduce-log-analysis.jar model.MainMapReduceApp input/logs.csv output/

## Clases Implementadas
1. Frecuencia de URLs: Calcula la cantidad de visitas por URL
2. Errores HTTP: Contabiliza códigos de error en los registros


