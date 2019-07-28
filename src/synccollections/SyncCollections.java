package synccollections;

import bdd.Fsite;
import bdd.FsiteDAO;
import bdd.Ftype;
import bdd.FtypeDAO;
import bdd.Furgent;
import bdd.FurgentDAO;
import bkgpi2a.Company;
import bkgpi2a.Identifiants;
import bkgpi2a.Patrimony;
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
 * @version 0.06
 */
public class SyncCollections {

    /**
     * mgoDbServerType : prod pour le serveur de production, pre-prod pour le
     * serveur de pré-production. Valeur par défaut : pre-prod.
     */
    private String mgoDbServerType = "pre-prod";

    /**
     * ifxDbServerType : prod pour le serveur de production, pre-prod pour le
     * serveur de pré-production. Valeur par défaut : pre-prod.
     */
    private String ifxDbServerType = "pre-prod";

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
     * debugMode : fonctionnement du programme en mode debug (true/false).
     * Valeur par défaut : false.
     */
    private static boolean debugMode = false;

    /**
     * testMode : fonctionnement du programme en mode test (true/false). Valeur
     * par défaut : false.
     */
    private static boolean testMode = false;

    /**
     * Constructeur de la classe SyncCollections
     * <p>
     * Les arguments en ligne de commande permettent de changer le mode de
     * fonctionnement.</p><ul>
     * <li>-mgodb prod|pre-prod : référence à la base de données MongoDB, par
     * défaut désigne la base de données de pré-production. Voir fichier
     * myDatabases.prop (optionnel).</li>
     * <li>-ifxdb prod|pre-prod|prod2|pre-prod2 : référence à la base de données
     * Informix, par défaut désigne la base de données de pré-production. Voir
     * fichier myDatabases.prop (optionnel).</li>
     * <li>-patrimonies clientCompanyUuid : demande la synchronisation des
     * patrimoines du client ayant l'identifiant clientCompanyUuid (paramètre
     * optionnel).</li>
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
        DBServer mgoServer;
        DBServer ifxServer;
        GetArgs getArgs;
        MongoClient mongoClient;
        MongoDatabase mongoDatabase;
        DBManager informixDbManager;
        Connection informixConnection;

        System.out.println("Création d'une instance de SyncCollections ...");

        System.out.println("Analyse des arguments de la ligne de commande ...");
        getArgs = new GetArgs(args);
        setMgoDbServerType(getArgs.getMongoDbServerType());
        setIfxDbServerType(getArgs.getInformixDbServerType());
        debugMode = getArgs.getDebugMode();
        testMode = getArgs.getTestMode();
        System.out.println("Argument(s) en ligne de commande lus().");

        System.out.println("Lecture des paramètres d'exécution ...");
        applicationProperties = new ApplicationProperties("MyDatabases.prop");
        System.out.println("Paramètres d'exécution lus.");

        System.out.println("Lecture des paramètres du serveur Mongo ...");
        mgoServer = new DBServer(mgoDbServerType, "mgodb", applicationProperties);
        System.out.println("Paramètres du serveur Mongo lus.");
        if (debugMode) {
            System.out.println(mgoServer);
        }

        System.out.println("Lecture des paramètres du serveur Informix ...");
        ifxServer = new DBServer(ifxDbServerType, "ifxdb", applicationProperties);
        System.out.println("Paramètres du serveur Informix lus.");
        if (debugMode) {
            System.out.println(ifxServer);
        }

        System.out.println("Ouverture de la connexion au serveur MongoDb : " + mgoServer.getName());
        mongoClient = new MongoClient(mgoServer.getIpAddress(), (int) mgoServer.getPortNumber());

        System.out.println("Connexion à la base de données : " + mgoServer.getDbName());
        mongoDatabase = mongoClient.getDatabase(mgoServer.getDbName());

        System.out.println("Ouverture de la connexion au serveur Informix : " + ifxServer.getName());
        informixDbManager = new DBManager(ifxServer);

        System.out.println("Connexion à la base de données : " + ifxServer.getDbName());
        informixConnection = informixDbManager.getConnection();

//        System.out.println("Synchronisation des sociétés ...");
//        syncCompanies(mongoDatabase, informixConnection);
//        System.out.println("Synchronisation des sociétés ...");
//        splTester(informixConnection);
        if (getArgs.getReadPatrimonies()) {
            syncPatrimonies(mongoDatabase, informixConnection, getArgs.getClientCompanyUuid());
        }

    }

    /**
     * Méthode pour tester l'utilisation de procédures stockées depuis Java
     *
     * @param informixConnection connexion à la base de données Informix
     */
    public void splTester(Connection informixConnection) {
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        Timestamp timestamp;

        timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("timestamp:" + timestamp);
        try {
            preparedStatement = informixConnection.prepareStatement("{call addMessage(?, ?, ?)}");
            preparedStatement.setInt(1, 4956554);
            preparedStatement.setString(2, "Maître Corbeau, sur un arbre perché, Tenait en son bec un fromage. Maître Renard, par l'odeur alléché, Lui tint à peu près ce langage : 'Hé ! bonjour, Monsieur du Corbeau. Que vous êtes joli ! que vous me semblez beau ! Sans mentir, si votre ramage Se rapporte à votre plumage, Vous êtes le Phénix des hôtes de ces bois.' A ces mots le Corbeau ne se sent pas de joie ; Et pour montrer sa belle voix, Il ouvre un large bec, laisse tomber sa proie. Le Renard s'en saisit, et dit : 'Mon bon Monsieur, Apprenez que tout flatteur Vit aux dépens de celui qui l'écoute : Cette leçon vaut bien un fromage, sans doute. Le Corbeau, honteux et confus, Jura, mais un peu tard, qu'on ne l'y prendrait plus.");
            preparedStatement.setTimestamp(3, timestamp);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("retcode:" + resultSet.getInt(1));
                System.out.println("nbtrials:" + resultSet.getInt(2));
            }
            resultSet.close();
            preparedStatement.close();

//            preparedStatement = informixConnection.prepareStatement("{call findCall(?, ?)}");
//            preparedStatement.setString(1, "49");
//            preparedStatement.setInt(2, 635);
//            resultSet = preparedStatement.executeQuery();
//            while (resultSet.next()) {
//                System.out.println("retcode:" + resultSet.getInt(1) + 
//                        ", table:" + resultSet.getInt(2) + 
//                        ", cnum:" + resultSet.getInt(3));
//            }
//            resultSet.close();
//            preparedStatement.close();
        } catch (SQLException ex) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Méthode pour synchroniser les patrimoines par rapport à la base de
     * données Informix.
     */
    private void syncPatrimonies(MongoDatabase mongoDatabase, Connection informixConnection, String clientCompanyUuid) {
        Furgent furgent;
        FurgentDAO furgentDAO;
        Ftype ftype;
        FtypeDAO ftypeDAO;
        Fsite fsite;
        FsiteDAO fsiteDAO;
        int nbSite;
        Patrimony patrimony;
        ObjectMapper objectMapper;
        MongoCollection<Document> collection;
        MongoCursor<Document> cursor;
        BasicDBObject filter;
        String aggregateUid;
        int nbPatrimoniesFound;
        int nbPatrimoniesNotFound;

        objectMapper = new ObjectMapper();

        collection = mongoDatabase.getCollection("patrimonies");
        System.out.println(collection.count() + " patrimonie(s) dans la base MongoDb");

        try {
            furgentDAO = new FurgentDAO(informixConnection);
            if (clientCompanyUuid != null) {
                furgentDAO.filterByUuid(clientCompanyUuid);
            }
            furgentDAO.orderBy("unum");
            System.out.println("  SelectStatement=" + furgentDAO.getSelectStatement());
            furgentDAO.setSelectPreparedStatement();
            if ((furgent = furgentDAO.select()) != null) {
                System.out.println("Client:" + furgent.getUname() + ", unum=" + furgent.getUnum() + ", uuid:" + furgent.getUuid());
                ftypeDAO = new FtypeDAO(informixConnection);
                ftypeDAO.filterByCode(furgent.getUnum(), 1);
                System.out.println("  SelectStatement=" + ftypeDAO.getSelectStatement());
                ftypeDAO.setSelectPreparedStatement();
                if ((ftype = ftypeDAO.select()) != null) {
                    System.out.println("Raison d'appel:" + ftype.getTtypename() + ", ttnum:" + ftype.getTtnum());
                    fsiteDAO = new FsiteDAO(informixConnection);
                    fsiteDAO.filterByType(furgent.getUnum(), ftype.getTtnum());
                    System.out.println("  SelectStatement=" + fsiteDAO.getSelectStatement());
                    fsiteDAO.setSelectPreparedStatement();
                    nbSite = 0;
                    nbPatrimoniesFound = 0;
                    nbPatrimoniesNotFound = 0;
                    while ((fsite = fsiteDAO.select()) != null) {
                        nbSite++;
                        System.out.println(nbSite + ", ref:" + fsite.getS3number2()
                                + ", label:" + fsite.getS3address()
                                + " " + fsite.getS3poscode()
                                + " " + fsite.getS3city());
                        aggregateUid = Md5.encode("s3:" + furgent.getUnum() + ":" + fsite.getS3number2());
                        System.out.println("  aggregateUid:" + aggregateUid);
                        filter = new BasicDBObject("uid", aggregateUid);
                        cursor = collection.find(filter).iterator();
                        if (cursor.hasNext()) {
                            patrimony = objectMapper.readValue(cursor.next().toJson(), Patrimony.class);
                            System.out.println("  patrimoine trouvé, ref:" + patrimony.getRef() + ", label:" + patrimony.getLabel() + ", uid:" + patrimony.getUid());
                            nbPatrimoniesFound++;
                        } else {
                            System.out.println("  patrimoine non trouvé");
                            nbPatrimoniesNotFound++;
                        }
                    }
                    if (nbSite == 0) {
                        System.out.println("Erreur : aucun site trouvé pour uuid:" + clientCompanyUuid);
                    } else {
                        System.out.println(nbSite + " site(s) trouvé(s) pour uuid:" + clientCompanyUuid);
                        System.out.println(nbPatrimoniesFound + " site(s) trouvé(s) dans la base Mongo");
                        System.out.println(nbPatrimoniesNotFound + " site(s) non trouvé(s) dans la base Mongo");
                    }
                    fsiteDAO.closeSelectPreparedStatement();
                } else {
                    System.out.println("Erreur : raison d'appel n°1 non trouvé pour uuid:" + clientCompanyUuid);
                }
                ftypeDAO.closeSelectPreparedStatement();
            } else {
                System.out.println("Erreur : client non trouvé pour uuid:" + clientCompanyUuid);
            }
            furgentDAO.closeSelectPreparedStatement();
        } catch (ClassNotFoundException exception) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, exception);
        } catch (SQLException exception) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, exception);
        } catch (IOException exception) {
            Logger.getLogger(SyncCollections.class.getName()).log(Level.SEVERE, null, exception);
        }
    }

    /**
     * Méthode pour synchroniser les clients par rapport à la base de données
     * Informix.
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
     * @param mgoDbServerType définit le serveur Web
     */
    private void setMgoDbServerType(String mgoDbServerType) {
        this.mgoDbServerType = mgoDbServerType;
    }

    /**
     * @param ifxDbServerType définit le serveur de base de données
     */
    private void setIfxDbServerType(String ifxDbServerType) {
        this.ifxDbServerType = ifxDbServerType;
    }

    /**
     * @return mgoDbServerType le serveur web
     */
    private String getMgoDbServerType() {
        return (mgoDbServerType);
    }

    /**
     * @return ifxDbServerType le serveur de base de données
     */
    private String getIfxDbServerType() {
        return (ifxDbServerType);
    }

    /**
     * Retourne le contenu de SyncCollections
     *
     * @return retourne le contenu de SyncCollections
     */
    @Override
    public String toString() {
        return "SyncCollections:{"
                + "mgodb:" + getMgoDbServerType()
                + ", ifxdb:" + getIfxDbServerType()
                + "}";
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
    /**
     * @param debugMode : fonctionnement du programme en mode debug
     * (true/false).
     */
    public void setDebugMode(boolean debugMode) {
        SyncCollections.debugMode = debugMode;
    }

    /**
     * @param testMode : fonctionnement du programme en mode test (true/false).
     */
    public void setTestMode(boolean testMode) {
        SyncCollections.testMode = testMode;
    }

    /**
     * @return debugMode : retourne le mode de fonctionnement debug.
     */
    public boolean getDebugMode() {
        return (debugMode);
    }

    /**
     * @return testMode : retourne le mode de fonctionnement test.
     */
    public boolean getTestMode() {
        return (testMode);
    }
}
