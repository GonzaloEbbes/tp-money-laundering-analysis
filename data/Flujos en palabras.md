# Requerimientos Funcionales:

Se solicita un sistema distribuido que analice el extracto de transacciones realizadas entre  
cuentas bancarias en busca de anomalías.  
● Se debe obtener:  
1\. Cuenta de origen, cuenta de destino y monto para transacciones USD menores a 50\.  
2\. Nombre de banco, cuenta de origen y monto de la max. transacción USD de cada banco.  
3\. Cuenta de origen y monto de transacciones USD en el período \[2022-09-06, 2022-09-15\]  
con monto menor a 1 centésimo del promedio encontrado para el mismo formato de  
pago en el período \[2022-09-01, 2022-09-05\]  
4\. Cuentas que cumplan con el patrón scatter-gather con una sola cuenta de separación,  
para cuentas que hayan realizado y cuya cuenta de origen haya realizado transferencias  
USD hacia entre 5 cuentas distintas dentro del período \[2022-09-01, 2022-09-05\]  
5\. Cantidad de transacciones del período \[2022-09-01, 2022-09-05\] con formato de pago  
"Wire" o "ACH" cuyo monto convertido a USD sea menor a 1

—  
SOLUCION

# Listado de workers: transfer data controller, CurrencyFilter, **AmountFilter, MapMaxAmountPerBank, JoinMaxAmountPerBank, FilterDateWindow, DynamicAmountFilter, MapAverage, JoinAverage, MapScatterGather, AggregationScatterGather, JoinScatterGather, PayFormatFilter, CurrencyConverter, TransferCounter**

# Query 1: 

Es un flujo de filtrado lineal.

El transfer data controller le envía la transacción al worker CurrencyFilter con la tag de Grupo: 1\.  
El worker CurrencyFilter recibe la transacción,  revisa que la transacción ocurra en dólares (descarta las que no), y luego , como es del grupo 1, lo envía a la cola del worker AmountFilter.   
El worker **AmountFilter** recibe la transacción y filtra montos mayores a 50 USD (porque el dato tiene la regla del grupo 1). 

Los workers usados son Stateless, no se requiere almacenamiento. 

# Query 2: 

El transfer data controller le envía la transacción al worker CurrencyFilter con la tag de Grupo: 2\.  
El worker **CurrencyFilter** recibe la transacción,  revisa que la transacción ocurra en dólares (descarta las que no), y luego, como es del grupo 2, lo envía a la cola del worker MapMaxAmountPerBank.  
El worker **MapMaxAmountPerBank** recibe la transacción y deberá actualizar un diccionario propio, agrupado por banco, almacenando la transacción máxima encontrada. Cuando finaliza de procesar todas las transacciones, reenvia al worker JoinMaxAmountPerBank los totales parciales de cada instancia de los MapMaxAmountPerBank  
El worker **JoinMaxAmountPerBank** recolecta los datos de maximos parciales y calcula el maximo total por banco.

Envia los datos finales

NUEVO DISEÑO: DataPerBankRedirector

Es Stateful, por lo que deberá ir bajando a disco las actualizaciones cada cierta cantidad de tiempo/transacciones procesadas.

#  Query 3:

 Procesa 2 flujos que actúan en paralelo 

El transfer data controller le envía la transacción al worker **FilterDateWindow** con la tag de Grupo: 3\_A.

El worker **FilterDateWindow** del grupo 3\_A recibe la transacción y revisa que la transacción ocurra en la primera ventana de tiempo (\[2022-09-06, 2022-09-15\]), luego, como es del grupo 3\_A, lo envía a la cola del worker CurrencyFilter.  
El worker **FilterDateWindow** del grupo 3\_B recibe la transacción y revisa que la transacción ocurra en la segunda ventana de tiempo (\[2022-09-01, 2022-09-05\]), luego, como es del grupo 3\_B, lo envía a la cola del worker CurrencyFilter.  
El worker **CurrencyFilter**, para ambos grupos, recibe la transacción y revisa que la transacción ocurra en dólares, luego,  a los del grupo 3\_A los envía a la cola del worker MapAverage, y a los del grupo 3\_B los envía a la cola del worker **DynamicAmountFilter**

Las instancias **MapAverage**, van calculando tanto el amount acumulado de las transacciones como el total de transacciones, agrupando por formato de pago. Una vez que termina de procesar todo, envía los datos a una cola de la cual procesarán los workers **JoinAverage**.

Los **JoinAverage** reciben los acumulados y total de transacciones parciales. Lo que hace es juntar todos los datos y calcular el promedio por medio de pago. Cuando obtiene los promedios, los envía a **DynamicAmountFilter**

El worker **DynamicAmountFilter** queda en espera con respecto a los datos del grupo 3\_B (en realidad los lee y los guarda en disco :) ), hasta tanto y en cuanto no termina de procesar los datos provenientes de **JoinAverage** (3\_A). Cuando eso ocurre, filtra los datos de 3\_A para cumplir que sean el 1% del promedio correspondiente a cada formato de pago (que esto datos los recibi del **JoinAverage**)

|  | wire | ach | cheque | etc | etc |
| :---- | :---- | :---- | :---- | :---- | :---- |
| Sum |  |  |  |  |  |
| Len |  |  |  |  |  |

El worker Avg es Stateful, por lo que deberá ir bajando a disco las actualizaciones cada cierta cantidad de tiempo/transacciones procesadas.

# Query 4: Patrón Scatter-Gather

El transfer data controller le envía la transacción al worker **FilterDateWindow** con la tag de Grupo: 4\.

El worker **FilterDateWindow** recibe la transacción y revisa que la transacción ocurra en la ventana de tiempo indicada (\[2022-09-01, 2022-09-05\]). Luego, como es del grupo 4, lo envía a la cola del worker CurrencyFilter.  
El worker **CurrencyFilter**, para ambos grupos, recibe la transacción y revisa que la transacción ocurra en dólares, luego, como es del grupo 4, lo envía a la cola del worker **MapScatterGather**.

El worker **MapScatterGather**, al recibir una transacción, actualiza un diccionario agregando (origen, destinos) y (destino, origenes) \- lo crea si no existe, lo appendea en caso de existir. Los diccionarios son Fanout (clave origen y valor vector de destinos) y Fanin (clave destino y valor vector de origenes).

Una vez que se termina de procesar todo, a traves de un metodo de hasheo (por separado, a la cuenta origen para fanout y a la cuenta destino para fanin: cosa de que los datos de una misma cuenta vayan al mismo aggregator) se envían los datos a AggregationScatterGather

El worker **AggregationScatterGather** combina todos los diccionarios con los datos del worker anterior. Solo pasan los datos de ambos diccionarios que compartan 5 valores o más, en ese caso irían a la cola del worker JoinerScatterGather

El worker **JoinerScatterGather** recibe los diccionarios resultantes y los matchea devolviendo al menos 7-uplas de los casos hallados, reenviandolos/almacenandolos.

# Query 5: 

El transfer data controller le envía la transacción al worker FilterDateWindow por el grupo 5\.

El worker **FilterDateWindow** recibe la transacción y revisa que la transacción ocurra en la ventana de tiempo ( \[2022-09-01, 2022-09-05\] ). Luego, como es del grupo 5, lo envía a la cola del worker **PayFormatFilter**.

El worker **PayFormatFilter**, recibe la transacción y revisa que la transacción ocurra con los formatos de pago especificados ("Wire" o "ACH" ). Luego, como es del grupo 5,  revisa si es o no es USD. 

* Es USD: la envía a la cola del worker **AmountFilter**  
* No es USD: la envía a la cola del worker **CurrencyConverter**

El worker **CurrencyConverter** convierte el valor de la transacción a dólares y lo envía a la cola del worker **AmountFilter**.

El worker **AmountFilter** recibe la transacción y filtra montos mayores a 1 USD. Como es del grupo 5, lo envía a un **TransferCounter**, luego eso se reenvia/almacena.

(sino sería una instancia sum y otra de join)  
VISTA ESCENARIOS

- Diagrama de “cliente envia todo esto” y recibe las rtas a las 5 queries

VISTA LOGICA

DAG mostrando el flujo de todas las reglas, donde se organiza en “metaworkers” (**MapScatterGather AggregationScatterGather JoinerScatterGather sino ScatherGather**)

VISTA PROCESOS

- Diagrama de secuencia y de actividad de regla 1  
- Diagrama de secuencia y de actividad de regla 2  
- Diagrama de secuencia y de actividad de regla 3  
- Diagrama de secuencia y de actividad de regla 4  
- Diagrama de secuencia y de actividad de regla 5

El diagrama de Actividades representa como cajitas cada worker sin adentrarse demasiado en que hace cada worker. El diagrama de secuencia va mas al hueso (especifica exactamente que se esta haciendo por adentro)

VISTA FÍSICA

- Diagrama de Robustez para regla 1  
- Diagrama de Robustez para regla 2  
- Diagrama de Robustez para regla 3  
- Diagrama de Robustez para regla 4  
- Diagrama de Robustez para regla 5

(no importa si repiten componentes entre reglas)

- Diagrama general de Despliegue: en el habrá por cada worker un nodo. Se muestras las n instancias posibles. Puede dividirse en varios

VISTA DESARROLLO

- Diagrama de paquetes para regla 1  
- Diagrama de paquetes para regla 2  
- Diagrama de paquetes para regla 3  
- Diagrama de paquetes para regla 4  
- Diagrama de paquetes para regla 5

Gaby: Vista fisica y vista escenarios (6)  
Martin: vista desarrollo y vista logica (6)  
Gonza: Vista Procesos (10)  
Final \-\> Compilacion en ppt? informe?  
LOGICA EOF  
1- se envia un eof desde el sender   
2- El primer worker que recibe eof arma un array de bools indicando los workers que recibieron el eof por posicion del array  
3- El mensaje se reinserta al final de la cola, hasta que todos los elementos sean true

Esto es eventualmente consistente…

(implica una cola de trabajo)  
