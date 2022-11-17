# cluster-hadoop-docker

Este repositorio está basado en https://github.com/loicmathieu/docker-cdh

Se utiliza en diversas asignaturas de la ETSI Informática de la Universidad Nacional de Educación a Distancia (UNED) de España.

Contiene instrucciones para el despliegue de un cluster de Hadoop, compuesto de 1 namenode, 1 yarmaster y 4 datanodes. Este cluster tiene instalados, entre otros paquetes, Python 3, Jupyter notebooks y mrjob.

## Requerimientos

Para desplegar el cluster, es necesario tener instalado en nuestro ordenador el software Docker (https://docs.docker.com/install/) y docker-compose (https://docs.docker.com/compose/install/)

## Despliegue

Para desplegar el cluster, tras descargar o clonar el repositorio en tu ordenador, ejecuta la orden:

```
$ docker-compose up -d
```

Tras esto, podrás ver los contenedores ejecutándose, como se ve abajo:

```
$ docker ps
CONTAINER ID        IMAGE                                   COMMAND                  CREATED             STATUS              PORTS                                                                        NAMES
4c16d001dc51        accaminero/cloudera-hadoop-datanodep3   "/usr/bin/supervisor…"   About an hour ago   Up About an hour    8042/tcp, 50020/tcp, 50075/tcp                                               datanode3
cf7083bc58b5        accaminero/cloudera-hadoop-namenodep3   "bash -c 'wget https…"   About an hour ago   Up About an hour    0.0.0.0:8020->8020/tcp                                                       namenode
6532dc0a47d7        accaminero/cloudera-hadoop-datanodep3   "/usr/bin/supervisor…"   About an hour ago   Up About an hour    0.0.0.0:8042->8042/tcp, 0.0.0.0:50020->50020/tcp, 0.0.0.0:50075->50075/tcp   datanode1
d4c276ae2df4        accaminero/cloudera-hadoop-datanodep3   "/usr/bin/supervisor…"   About an hour ago   Up About an hour    8042/tcp, 50020/tcp, 50075/tcp                                               datanode2
b0ec396f34f4        accaminero/cloudera-hadoop-datanodep3   "/usr/bin/supervisor…"   About an hour ago   Up About an hour    8042/tcp, 50020/tcp, 50075/tcp                                               datanode4
fb0a9c55f254        loicmathieu/cloudera-cdh-yarnmaster     "/usr/bin/supervisor…"   About an hour ago   Up About an hour    0.0.0.0:8032->8032/tcp, 8080/tcp                                             yarnmaster
```

Para que el trabajo con el cluster sea más sencillo, es conveniente incluir el mapeo entre direcciones IP de cada contenedor y su hostname. Esto se hace modificando el fichero /etc/hosts de Linux (o su equivalente en otros sistemas operativos) para que incluya dicho mapeo.

Para averiguar la dirección IP de un contenedor, ejecutamos la siguiente orden para cada uno de los contenedores:

```
$  
            "SecondaryIPAddresses": null,
            "IPAddress": "",
                    "IPAddress": "172.28.0.6",
```

Una vez tengamos la dirección IP de cada contenedor, incluiremos en el fichero /etc/hosts (o su equivalente en sistemas operativos distintos de Linux) unas líneas como las siguientes:

```
$ cat /etc/hosts
...

172.28.0.6    namenode
172.28.0.4      yarnmaster
172.28.0.7    datanode1
172.28.0.5      datanode2
172.28.0.2      datanode3
172.28.0.3      datanode4

...
```

Tras esto, podemos acceder al servidor Jupyter notebooks a través del navegador, con la siguiente URL: namenode:8889

## Contacto

Para cualquier consulta, mis datos de contacto son los siguientes:

```
Dr. Agustín C. Caminero Herráez
Dpto. de Sistemas de Comunicación y Control. 
ETSI Informática, UNED.
C/ Juan del Rosal, 16. 28040 Madrid. Spain.
Teléfono: 0034 91398 9468
Email: accaminero (at) scc (dot) uned (dot) es
Twitter: @accaminero 
Web: https://portal.uned.es/portal/page?_pageid=93,37430516&_dad=portal&_schema=PORTAL
```
