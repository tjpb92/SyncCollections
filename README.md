# SyncCollections
Programme Java permettant de synchroniser les collections d'une base de données MongoDB par rapport à une base de données Informix

## Utilisation:
```
java SyncCollections [-mgodb mongodb] [-ifxdb informixdb] [-patrimonies clientCompanyUuid] [-d] [-t] 
```
où :
* ```-mgodb prod|pre-prod``` est la référence à la base de données MongoDB, par défaut désigne la base de données de pré-production. Voir fichier *MyDatabases.prop* (optionnel).
* ```-ifxdb prod|pre-prod|prod2|pre-prod2``` est la référence à la base de données Informix, par défaut désigne la base de données de pré-production. Voir fichier *MyDatabases.prop* (optionnel).
* ```-patrimonies clientCompanyUuid``` demande la synchronisation des patrimoines du client ayant l'identifiant clientCompanyUuid (paramètre optionnel).
* ```-d``` le programme s'exécute en mode débug, il est beaucoup plus verbeux. Désactivé par défaut (paramètre optionnel).
* ```-t``` le programme s'exécute en mode test, les transcations en base de données ne sont pas faites. Désactivé par défaut (paramètre optionnel).

## Pré-requis :
- Java 6 ou supérieur.
- JDBC Informix
- JDBC MySql
- Driver MongoDB
- Librairie Jackson

## Fichier des paramètres : 

Ce fichier permet de spécifier les paramètres d'accès aux différentes bases de données.

A adapter selon les implémentations locales.

Ce fichier est nommé : *MyDatabases.prop*.

Le fichier *MyDatabases_Example.prop* est fourni à titre d'exemple.
