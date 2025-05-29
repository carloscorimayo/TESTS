# `ra_cdt_functions_f00_config.py`

Este script Python, `ra_cdt_functions_f00_config.py`, es una parte fundamental de nuestro proyecto, diseñado para manejar de forma **flexible la configuración** del entorno de ejecución. Su principal objetivo es proporcionar una interfaz consistente para acceder a los parámetros de configuración, adaptándose automáticamente si el código se ejecuta en **Airflow** o en un entorno de **desarrollo/pruebas (Notebook o PYTest)**.

---

## Propósito General

El archivo permite que el resto del proyecto recupere los valores de configuración necesarios (como rutas de archivos, indicadores de procesamiento, etc.) sin preocuparse por la fuente subyacente de esos valores. Esto facilita el desarrollo local y las pruebas, a la vez que asegura una integración fluida con nuestro flujo de trabajo de Airflow.

---

## ¿Cómo funciona?

La lógica central reside en la función `get_config(context)`. Esta función examina el argumento `context` para determinar el entorno actual:

### 1. Ejecución en Airflow

Cuando el script se ejecuta como parte de un **DAG de Airflow**, el `context` proporcionado por Airflow (que no será la cadena `'TEST_OR_NOTEBOOK'`) contiene la información del "job request" (`context['jr']`).

* El script toma esta información, la convierte en un **DataFrame de Pandas** y la guarda temporalmente en un archivo CSV llamado `jobrequest.csv` dentro del espacio de trabajo (`get_workspace()`).
* Se define una **clase interna `get_config`** que imita la forma en que Airflow normalmente proporciona la configuración. El método estático `get(parametro)` de esta clase lee `jobrequest.csv` y extrae el valor del parámetro solicitado.
* Incluye lógica especial para el parámetro `'DG_BFF_file_FromAzureBlobStorage'` y maneja la conversión de valores booleanos a las cadenas `"true"` o `"false"`.

### 2. Ejecución en Notebook o PYTest

Si el script se ejecuta en un entorno de desarrollo interactivo como un **Jupyter Notebook** o durante **pruebas con PYTest** (indicado por `context == 'TEST_OR_NOTEBOOK'`), la configuración se obtiene de un archivo YAML.

* En este caso, una **clase interna `get_config`** diferente se encarga de leer los parámetros.
* El método estático `get(parametro)` de esta clase abre el archivo **`PYTEST_job_request.yaml`**, busca el parámetro solicitado bajo la sección `configuration` y devuelve su valor.
* También maneja la conversión de valores booleanos a las cadenas `"true"` o `"false"`.

---

## Uso

Para obtener un parámetro de configuración, simplemente importa la función `get_config` y úsala de la siguiente manera:

```python
from ra_cdt_functions_f00_config import get_config

# Si estás en un Notebook o haciendo pruebas:
config_loader = get_config('TEST_OR_NOTEBOOK')
some_parameter = config_loader.get('nombre_de_tu_parametro')

# Si este código es parte de un DAG de Airflow, el contexto se pasaría automáticamente:
# Por ejemplo, en un operador de Python:
# def my_task(ds, **kwargs):
#     config_loader = get_config(kwargs) # kwargs contiene el 'jr' en su contexto
#     some_parameter = config_loader.get('nombre_de_tu_parametro')
#     # ... usa some_parameter
