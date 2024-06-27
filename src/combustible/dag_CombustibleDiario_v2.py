#
# Este DAG fue desarrollado para testear la ejecución directa de códigos python sin necesidad de .bat 
# usando los operadores PythonOperator y PythonVirtualenvOperator
#
##

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator



# Defining some basic arguments
default_args = {
   'owner': 'R-Studio',
   'depends_on_past': False,
   'email':'christian.zarria@chilecompra.cl, hector.garrido@chilecompra.cl',
   'email_on_failure':True,
   'start_date': datetime(2019, 1, 2),
   'retries': 0,
   }

with DAG(
       'Dag_Combustibles_Diario_v2',
       #schedule_interval='@monthly',
       schedule_interval= '00 12 * * *', # todos los dias a las 22 hrs
       catchup=False,
       default_args=default_args
       ) as dag:



    t1 = BashOperator(
          task_id='Procesa_CombustibleDiario_v2',
          bash_command="""
          cd /c/Users/cl15002627k/AirflowCodes/Combustibles_v2/
          python3 FuelDailyUpdates.py
          """)

    fecha= datetime.today().strftime('%d-%m-%Y')

    send_mail = EmailOperator( 
         task_id='send_email', 
         to='christian.zarria@chilecompra.cl, hector.garrido@chilecompra.cl', 
         subject='[AIRFLOW] Ejecucion Combustibles Diario V2 Correcta', 
         html_content="Fecha:"+fecha
         
                  )   

        
        
    # patron de ejecución
    t1>>send_mail
