## R & Hadoop for data analysis with MapReduce

## TCPDump Logs

L'elaborato implementa l'algoritmo MapReduce con Hadoop ed R.
In particolare si propongono due esempi, uno basato sullo streaming, l'altro usa la libreria rhadoop.
Un terzo esempio, basato su RHipe, non e' stato implementato a causa di un bug aperto sulla libreria che la rende incompatibile con la versione di hadoop fornita nel container utilizzato.

L'installazione di R con le relative librerie e il deploy di hadoop e' stato automatizzato attraverso un container Docker derivato da dockerhub:sequenceiq/hadoop-docker

## Statistiche fornite
- i primi 10 server che ricevono più pacchetti;
- i primi 10 client che hanno inviato piu' byte;
- la percentuale di pacchetti piccoli (< 512 bytes)
- la percentuale di paccehtti grandi (>= 512 bytes)
- il numero totale di byte inviati ogni 5 minuti ad ogni server nell'intero periodo coperto dal file fornito

Di quest'ultima statistica viene salvato, in formato pdf, un grafico a linea.
La scelta del salvataggio diretto su pdf e' dovuta all'utilizzo necessario di R dentro il container con hadoop che non fornisce forwarding della gui verso l'host.


## Dependencies

* Hadoop
* devtools (R)
* klmr/modules (R)
* roxygen2 (R)
* linux-coreutils: (cat, less, sort... )
* docker

## Running

#### Hadoop (e Docker)

I test sul container sono stati eseguiti esclusivamente su sistemi operativi GNU/Linux, in partiolare su Ubuntu Xenial e Fedora 27.

Per il build del container:
```
    $ docker build -t rhadoop .
    $ docker run -v /ABSPATH/TO/THIS/REPO/:/code -it rhadoop /etc/bootstrap.sh -bash
```

Fare attenzione ai permessi e alle policy di SELinux.

### Streaming

Gli script per lo streaming possono essere eseguiti anche senza hadoop

```
    $ cd src/streaming
    $ cat myTcpDump.log | ./mapper.r | ./reducer.r

```

#### Con hadoop

Per eseguire il codice, dentro il container:
```
    # cd /code/
    # hdfs dfs -copyFromLocal $myTcpDumpLogFile $myTcpDumpLogFile
    # hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar \
        -file ./utils.r
        -file ./mapper.r    -mapper ./mapper.r \
        -file ./reducer.r   -reducer ./reducer.r \
        -input $MyTCPDumpLogFile -output log.log ' 
    # hdfs dfs -cat log.log/part* | less
```

In particolare $myTcpDumpLogFile e' il percorso del tcpdump.log, un esempio si trova in /samples: si puo' opportunamente replicare su se stesso per aumentarne la dimensione all'ordine dei GB; ne risulteranno statistiche uguali, ma e' il modo piu' semplice per avere un grosso dump da analizzare.

Alla fine, nel caso dello streaming, e' sempre necessario eliminare l'ouput prodotto con: 
```
    # hdfs dfs -rm log.log will delete the log directory if you need to restart the map-reduce as above.
```

in `src/streaming/script.sh` e' presente uno script sh per eseguire tutto quanto descritto sopra out-of-the-box.

## RHadoop

La stessa analisi e' prodotta usando rhadoop (solo questa versione salva il gafico in pdf).

```sh
    # cd src/rhadoop
    # hdfs dfs -copyFromLocal ../../samples/tcpdump.log tcpdumop.log 
    # ./rhadoop.r
```

Alla fine verranno stampate le statistiche sopra descritte e il pdf sara' salvato su `./plot.pdf`

## RHipe

Not working on Hadoop 2.7: https://github.com/delta-rho/RHIPE/issues/45

## Repository tree

```sh
.
├── docker
│   └── Dockerfile
├── README.md
├── samples
│   └── tcpdump.log
└── src
    ├── plots                       # Files di esempio per il plot
    │   ├── input.txt
    │   └── plot.r
    ├── rhadoop                     # Anlisi con Rhadoop
    │   ├── rhadoop_ex.r            # un primo esempio iniziale
    │   └── rhadoop.r               # Lo script finale
    ├── rhipe
    │   └── example.r               # esegue esclusivamente lo unit test di rhipe, fallendo, vedi sopra
    └── streaming
        ├── mapper.r                # il mapper
        ├── reducer.r               # il reducer
        ├── script.sh               # script di esecuzione out-of-the-box
        └── utils.r                 # un insieme di funzioni utili sia al reducer che al mapper

8 directories, 13 files
```
