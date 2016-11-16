package synccollections;

import bdd.Furgent;
import bdd.FurgentDAO;
import bkgpi2a.Company;
import bkgpi2a.Identifiants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import utils.ApplicationProperties;
import utils.DBManager;
import utils.DBServer;
import utils.DBServerException;
import utils.Md5;

/**
 * SyncCollections, Programme Java permettant de synchroniser les collections
 * d'une base de données MongoDB par rapport à une base de données Informix
 *
 * @author Thierry Baribaud.
 * @version 0.02
 */
public class SyncCollections {

    /**
     * mongoDbServerType : prod pour le serveur de production, pre-prod pour le
     * serveur de pré-production. Valeur par défaut : pre-prod.
     */
    private String mongoDbServerType = "pre-prod";

    /**
     * informixDbServerType : prod pour le serveur de production, pre-prod pour
     * le serveur de pré-production. Valeur par défaut : pre-prod.
     */
    private String informixDbServerType = "pre-prod";

    /**
     * mongoDbId : identifiants pour se connecter au serveur MongoDb courant.
     * Pas de valeur par défaut, ils doivent être fournis dans le fichier
     * MyDatabases.prop.
     */
    private Identifiants mongoDbId;

    /**
     * informixDbId : identifiants pour se connecter au serveur Informix
     * courant. Pas de valeur par défaut, ils doivent être fournis dans le
     * fichier MyDatabases.prop.
     */
    private Identifiants informixDbId;

    /**
     * Constructeur de la classe SyncCollections
     * <p>
     * Les arguments en ligne de commande permettent de changer le mode de
     * fonctionnement.</p><ul>
     * <li>-mgodb mongodb : référence à la base de données MongoDB, par défaut
     * désigne la base de données de développement. Voir fichier
     * myDatabases.prop (optionnel).</li>
     * <li>-ifxdb informixdb : référence à la base de données Informix, par
     * défaut désigne la base de données de développement. Voir fichier
     * myDatabases.prop (optionnel).</li>
     * <li>-d : le programme fonctionne en mode débug le rendant plus verbeux,
     * désactivé par défaut (optionnel).</li>
     * <li>-t : le programme fonctionne en mode de test, les transactions en
     * base de données ne sont pas exécutées, désactivé par défaut
     * (optionnel).</li>
     * </ul>
     *
     * @param args arguments de la ligne de commande.
     * @throws java.io.IOException en cas d'erreur d'entrée/sortie.
     * @throws utils.DBServerException en cas d'erreur avec le serveur de base
     * de données.
     * @throws GetArgsException en cas d'erreur avec les paramètres en ligne de
     * commande
     */
    public SyncCollections(String[] args) throws IOException,
            DBServerException, GetArgsException, Exception {

        ApplicationProperties applicationProperties;
        DBServer mongoServer;
        DBServer informixServer;
        GetArgs getArgs;
        MongoClient mongoClient;
        MongoDatabase mongoDatabase;
        DBManager informixDbManager;
        Connection informixConnection;

        System.out.println("Création d'une instance de SyncCollections ...");

        System.out.println("Analyse des arguments de la ligne de commande ...");
        getArgs = new GetArgs(args);
        setMongoDbServerType(getArgs.getMongoDbServerType());
        setInformixDbServerType(getArgs.getInformixDbServerType());

        System.out.println("Lecture des paramètres d'exécution ...");
        applicationProperties = new ApplicationProperties("MyDatabases.prop");

        System.out.println("Lecture des paramètres du serveur MongoDb ...");
        mongoServer = new DBServer(getMongoDbServerType(), "mgodb", applicationProperties);
        System.out.println(mongoServer);
//        setMongoDbId(applicationProperties);
//        System.out.println(getMongoDbId());

        System.out.println("Lecture des paramètres du serveur Informix ...");
        informixServer = new DBServer(getInformixDbServerType(), "ifxdb", applicationProperties);
        System.out.println(informixServer);
//        setInformixDbId(applicationProperties);
//        System.out.println(getInformixDbId());

        System.out.println("Ouverture de la connexion au serveur MongoDb : " + mongoServer.getName());
//        mongoClient = new MongoClient(mongoServer.getIpAddress(), (int) mongoServer.getPortNumber());

        System.out.println("Connexion à la base de données : " + mongoServer.getDbName());
//        mongoDatabase = mongoClient.getDatabase(mongoServer.getDbName());

        System.out.println("Ouverture de la connexion au serveur Informix : " + informixServer.getName());
        informixDbManager = new DBManager(informixServer);

        System.out.println("Connexion à la base de données : " + informixServer.getDbName());
        informixConnection = informixDbManager.getConnection();

        System.out.println("Synchronisation des sociétés ...");
//        syncCompanies(mongoDatabase, informixConnection);

        System.out.println("Synchronisation des sociétés ...");
        splTester(informixConnection);
    }

    /**
     * Méthode pour tester l'utilisation de procédures stockées depuis Java
     * @param informixConnection connexion  à la base de données Informix
     */
    public void splTester(Connection informixConnection) {
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        Timestamp timestamp;
        
        timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("timestamp:" + timestamp);
        try {
//            preparedStatement = informixConnection.prepareStatement("{call addMessage(?, ?, ?)}");
//            preparedStatement.setInt(1, 4828941);
//            preparedStatement.setString(2, "Les sanglots longs Des violons De l'automne Blessent mon coeur D'une langueur Monotone. Tout suffocant Et blême, quand Sonne l'heure, Je me souviens Des jours anciens Et je pleure Et je m'en vais Au vent mauvais Qui m'emporte Deçà, delà, Pareil à la Feuille morte.");
//            preparedStatement.setTimestamp(3, timestamp);
//            resultSet = preparedStatement.executeQuery();
//            while (resultSet.next()) {
//                System.out.println("retcode:" + resultSet.getInt(1));
//            }
//            resultSet.close();
//            preparedStatement.close();
            preparedStatement = informixConnection.prepareStatement("{call findCall(?, ?)}");
            preparedStatement.setString(1, "49");
            preparedStatement.setInt(2, 635);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("retcode:" + resultSet.getInt(1) + 
                        ", table:" + resultSet.getInt(2) + 
                        ", cnum:" + resultSet.getInt(3));
            }
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException ex) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Méthode pour synchroniser les clients par rapport à la base de données Informix.
     */
    private void syncCompanies(MongoDatabase mongoDatabase, Connection informixConnection) {
        Furgent emergencyService;
        FurgentDAO furgentDAO;
        int i;
        String aggregateUid;
        MongoCollection<Document> collection;
        MongoCursor<Document> cursor;
        int n;
        ObjectMapper objectMapper;
        Company company;
        int nbCompany;
        BasicDBObject filter;
        UpdateResult updateResult;

        objectMapper = new ObjectMapper();

        collection = mongoDatabase.getCollection("companies");
        System.out.println(collection.count() + " compagnie(s) dans la base MongoDb");

        try {
            furgentDAO = new FurgentDAO(informixConnection);
            furgentDAO.orderBy("unum");
            System.out.println("  SelectStatement=" + furgentDAO.getSelectStatement());
            furgentDAO.setSelectPreparedStatement();
            i = 0;
            nbCompany = 0;
            while ((emergencyService = furgentDAO.select()) != null) {
                i++;
                System.out.println("Client(" + i + ")=" + emergencyService);
                aggregateUid = Md5.encode("u:" + emergencyService.getUnum());
//                System.out.println("  aggregateUid:" + aggregateUid);
                filter = new BasicDBObject("uid", aggregateUid);
//                cursor = collection.find(filter).iterator();
//                if (cursor.hasNext()) {
//                    nbCompany++;
//                    company = objectMapper.readValue(cursor.next().toJson(), Company.class);
//                    updateResult = collection.updateOne(filter, new BasicDBObject("$set", new BasicDBObject("id", emergencyService.getUnum())));
//                    System.out.println("  trouvé : " + company.getLabel() + ", uid:" + company.getUid() + ", updateResult:" + updateResult);
//                }
                updateResult = collection.updateOne(filter, new BasicDBObject("$set", new BasicDBObject("id", emergencyService.getUnum())));
                if (updateResult.getMatchedCount() > 0) {
                    nbCompany++;
                    System.out.println("  trouvé : updateResult:" + updateResult);
                }
            }
            furgentDAO.closeSelectPreparedStatement();
            System.out.println(i + " client(s) lu(s), " + nbCompany + " société(s) trouvée(s)");

        } catch (ClassNotFoundException exception) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, exception);
        } catch (SQLException exception) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, exception);
        }

    }

    /**
     * @param mongoDbServerType définit le serveur Web
     */
    private void setMongoDbServerType(String mongoDbServerType) {
        this.mongoDbServerType = mongoDbServerType;
    }

    /**
     * @param informixDbServerType définit le serveur de base de données
     */
    private void setInformixDbServerType(String informixDbServerType) {
        this.informixDbServerType = informixDbServerType;
    }

    /**
     * @return mongoDbServerType le serveur web
     */
    private String getMongoDbServerType() {
        return (mongoDbServerType);
    }

    /**
     * @return informixDbServerType le serveur de base de données
     */
    private String getInformixDbServerType() {
        return (informixDbServerType);
    }

    /**
     * Retourne le contenu de SyncCollections
     *
     * @return retourne le contenu de SyncCollections
     */
    @Override
    public String toString() {
        return "SyncCollections:{webServer=" + getMongoDbServerType()
                + ", dbServer=" + getInformixDbServerType() + "}";
    }

    /**
     * Programme principal pour lancer SyncCollections.
     *
     * @param args paramètre de ligne de commande (cf. constructeur).
     */
    public static void main(String[] args) {

        SyncCollections syncCollections;

        System.out.println("Lancement de SyncCollections ...");

        try {
            syncCollections = new SyncCollections(args);
            System.out.println(syncCollections);
        } catch (Exception exception) {
            System.out.println("Problème lors de l'instanciation de SyncCollections");
            exception.printStackTrace();
        }

        System.out.println("Fin de SyncCollections");
    }

    /**
     * @return les identifiants pour accéder au serveur Web
     */
    public Identifiants getMongoDbId() {
        return mongoDbId;
    }

    /**
     * @param mongoDbId définit les identifiants pour accéder au serveur Web
     */
    public void setMongoDbId(Identifiants mongoDbId) {
        this.mongoDbId = mongoDbId;
    }

    /**
     * @param applicationProperties définit les identifiants pour accéder au
     * serveur Web
     * @throws WebServerException en cas d'erreur sur la lecteur des
     * identifiants
     */
//    public void setMongoDbId(ApplicationProperties applicationProperties) throws WebServerException {
//        String value;
//        Identifiants identifiants = new Identifiants();
//
//        value = applicationProperties.getProperty(getMongoDbServerType() + ".webserver.login");
//        if (value != null) {
//            identifiants.setLogin(value);
//        } else {
//            throw new WebServerException("Nom utilisateur pour l'accès Web non défini");
//        }
//
//        value = applicationProperties.getProperty(getMongoDbServerType() + ".webserver.passwd");
//        if (value != null) {
//            identifiants.setPassword(value);
//        } else {
//            throw new WebServerException("Mot de passe pour l'accès Web non défini");
//        }
//        SyncCollections.this.setMongoDbId(identifiants);
//    }
    /**
     * @return les identifiants pour accéder à la base de données
     */
    public Identifiants getInformixDbId() {
        return informixDbId;
    }

    /**
     * @param informixDbId définit les identifiants pour accéder à la base de
     * données
     */
    public void setInformixDbId(Identifiants informixDbId) {
        this.informixDbId = informixDbId;
    }

    /**
     * @param applicationProperties définit les identifiants pour accéder au
     * serveur Web
     * @throws WebServerException en cas d'erreur sur la lecteur des
     * identifiants
     */
//    public void setInformixDbId(ApplicationProperties applicationProperties) throws WebServerException {
//        String value;
//        Identifiants identifiants = new Identifiants();
//
//        value = applicationProperties.getProperty(getInformixDbServerType() + ".dbserver.login");
//        if (value != null) {
//            identifiants.setLogin(value);
//        } else {
//            throw new WebServerException("Nom utilisateur pour l'accès base de données non défini");
//        }
//
//        value = applicationProperties.getProperty(getInformixDbServerType() + ".dbserver.passwd");
//        if (value != null) {
//            identifiants.setPassword(value);
//        } else {
//            throw new WebServerException("Mot de passe pour l'accès base de données non défini");
//        }
//        SyncCollections.this.setInformixDbId(identifiants);
//    }
}
