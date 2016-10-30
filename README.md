# SyncCollections
Programme Java permettant de synchroniser les collections d'une base de donn�es MongoDB par rapport � une base de donn�es Informix

##Utilisation:
```
java SyncCollections [-mgodb mongodb] [-ifxdb informixdb] [-d] [-t] 
```
o� :
* ```-mgodb mongodb``` est la r�f�rence � la base de donn�es MongoDB, par d�faut d�signe la base de donn�es de pr�-production. Voir fichier *MyDatabases.prop* (optionnel).
* ```-ifxdb informixdb``` est la r�f�rence � la base de donn�es Informix, par d�faut d�signe la base de donn�es de pr�-production. Voir fichier *MyDatabases.prop* (optionnel).
* ```-d``` le programme s'ex�cute en mode d�bug, il est beaucoup plus verbeux. D�sactiv� par d�faut (param�tre optionnel).
* ```-t``` le programme s'ex�cute en mode test, les transcations en base de donn�es ne sont pas faites. D�sactiv� par d�faut (param�tre optionnel).

##Pr�-requis :
- Java 6 ou sup�rieur.
- JDBC Informix
- JDBC MySql
- Driver MongoDB
- Librairie Jackson

##Fichier des param�tres : 

Ce fichier permet de sp�cifier les param�tres d'acc�s aux diff�rentes bases de donn�es.

A adapter selon les impl�mentations locales.

Ce fichier est nomm� : *MyDatabases.prop*.

Le fichier *MyDatabases_Example.prop* est fourni � titre d'exemple.
