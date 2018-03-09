/*
   Apps Script: Accessing Google Cloud Datastore under a Service Account
   @author Martin Zeitler, https://plus.google.com/106963082057954766426
   @bitcoin 19uySyXrtqQ71PFZWHb2PxBwtNitg2Dp6b
*/

/* Service Account configuration file on Google Drive */
var CONFIG = "serviceaccount.json";

/* ID of an Entity, which is used by below functional tests */
var TEST_ID = "5558520099373056";

/* API wrapper */
var DatastoreApp = {
  
  debug:         true,
  scopes:        "https://www.googleapis.com/auth/datastore https://www.googleapis.com/auth/drive",
  baseUrl:       "https://datastore.googleapis.com/v1",
  httpMethod:    "POST",
  currentUrl:    false,
  transactionId: false,
  oauth:         false,
  projectId:     false,
  clientId:      false,
  clientEmail:   false,
  privateKey:    false,
  
  /* returns an instance */
  getInstance: function() {
    
    /* configure the client on demand */
    if(! this.config) {this.getConfig(CONFIG);}
    
    /* authenticate the client on demand */
    if(! this.oauth) {this.createService();}
    
    return this;
  },
  
  /* loads the configuration file from Google Drive */
  getConfig: function(filename) {
    var it = DriveApp.getFilesByName(filename);
    while (it.hasNext()) {
      var file = it.next();
      var data = JSON.parse(file.getAs("application/json").getDataAsString());
      this.projectId   = data.project_id;
      this.privateKey  = data.private_key;
      this.clientEmail = data.client_email;
      this.clientId    = data.client_id;
      continue;
    }
  },
  
  /* creates the oAuth2 service */
  createService: function() {
    this.oauth = OAuth2.createService("Datastore")
    .setTokenUrl("https://www.googleapis.com/oauth2/v4/token")
    .setPropertyStore(PropertiesService.getScriptProperties())
    // .setSubject(Session.getActiveUser().getEmail())
    .setPrivateKey(this.privateKey)
    .setIssuer(this.clientEmail)
    .setScope(this.scopes);
  },
  
  /* sets the request url per method */
  setCurrentUrl: function(method){
      this.currentUrl = this.baseUrl + "/projects/" + this.projectId + ":" + method;
  },
  
  /* gets the request options */
  getOptions: function(payload) {
    if (this.oauth.hasAccess()) {
      return {
        headers: {Authorization: 'Bearer ' + this.oauth.getAccessToken()},
        payload: JSON.stringify(payload),
        contentType: "application/json",
        muteHttpExceptions: true,
        method: this.httpMethod
      };
    }
  },
  
  /**
   * Queries for entities.
   * @param payload ~ partitionId, readOptions, query, gqlQuery
  **/
  runQuery: function(payload) {return this.request("runQuery", payload);},
  
  /**
   * Begins a new transaction.
   * @param payload ~ transactionOptions
  **/
  beginTransaction: function(payload) {return this.request("beginTransaction", payload);},
  
  /**
   * Commits a transaction, optionally creating, deleting or modifying some entities.
   * @param payload ~ mode, mutations, transaction
  **/
  commit: function(payload) {return this.request("commit", payload);},
  
  /**
   * Rolls back a transaction.
   * @param payload ~ transaction
  **/
  rollback: function(payload) {return this.request("rollback", payload);},
  
  /**
   * Allocates IDs for the given keys, which is useful for referencing an entity before it is inserted.
   * @param payload ~ keys
  **/
  allocateIds: function(payload) {return this.request("allocateIds", payload);},
  
  /**
   * Prevents the supplied keys' IDs from being auto-allocated by Cloud Datastore.
   * @param payload ~ databaseId, keys
  **/
  reserveIds: function(payload) {return this.request("reserveIds", payload);},
  
  /**
   * Looks up entities by key.
   * @param payload ~ readOptions, keys
  **/
  lookup: function(payload) {return this.request("lookup", payload);},
   
  /* API wrapper */
  request: function(method, payload) {

    if (this.oauth.hasAccess()) {

      /* configuring the request */
      var options = this.getOptions(payload);
      this.log(method + " > " + options.payload);
      this.setCurrentUrl(method);
      
      /* the individual api methods can be handled here */
      switch(method) {
        
        /* projects.runQuery */
        case "runQuery":
          break;
          
        /* projects.beginTransaction */
        case "beginTransaction":
          break;
        
        /* projects.commit */
        case "commit":
          if(! this.transactionId){
            this.log("cannot commit() while there is no ongoing transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
          }
          break;
        
        /* projects.rollback */
        case "rollback":
          if(! this.transactionId){
            this.log("cannot rollback() while there is no ongoing transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
          }
          break;
        
        /* projects.allocateIds */
        case "allocateIds":
          break;
        
        /* projects.reserveIds */
        case "reserveIds":
          break;
          
        /* projects.lookup */
        case "lookup":
          break;
        
        default:
          this.log("invalid api method: "+ method);
          return false;
      }
      
      /* execute the request */
      var response = UrlFetchApp.fetch(this.currentUrl, options);
      var result = JSON.parse(response.getContentText());
      this.handleResult(method, result);
      
      /* it returns the actual result of the request */
      return result;
      
    } else {
      this.log(this.oauth.getLastError());
      return false;
    }
  },
  
  /* handles the result */
  handleResult: function(method, result) {
    
    /* the individual api responses can be handled here */
    switch(method) {
        
      /* projects.runQuery */
      case "runQuery":
        if(typeof(result.batch) !== "undefined") {
          for(i=0; i < result.batch['entityResults'].length; i++) {
            this.log(JSON.stringify(result.batch['entityResults'][i]));
          }
        }
        break;
      
      /* projects.beginTransaction */
      case "beginTransaction":
        if(typeof(result.transaction) !== "undefined" && result.transaction != "") {
          this.log(method + " > " + result.transaction);
          this.transactionId = result.transaction;
        }
        break;
      
      /* projects.commit */
      case "commit":
        if(typeof(result.error) !== "undefined") {
          
          /* attempting to roll back the transaction in progress */
          this.rollback({"transaction": this.transactionId});
          
        } else {
          
          /* log the mutationResults, when debug is true */
          if(typeof(result.mutationResults) !== "undefined") {
            for(i=0; i < result.mutationResults.length; i++) {
              this.log(JSON.stringify(result.mutationResults[i]));
            }
          }
        }
        break;
      
      /* projects.rollback */
      case "rollback":
        /* resetting transaction in progress */
        this.transactionId = false;
        break;
      
      /* projects.allocateIds */
      case "allocateIds":
        
        /* log allocated keys, when debug is true */
        if(typeof(result.keys) !== "undefined") {
            for(i=0; i < result.keys.length; i++) {
              this.log(JSON.stringify(result.keys[i]));
            }
        }
        break;
      
      /* projects.reserveIds */
      case "reserveIds":
        
        break;
      
      /* projects.lookup */
      case "lookup":
        
        /* log found entities, when debug is true */
        if(typeof(result.found) !== "undefined") {
            for(i=0; i < result.found.length; i++) {
              this.log(JSON.stringify(result.found[i]));
            }
        }
        break;
    }
    
    if(typeof(result) !== "undefined") {
      
      /* log empty results */
      if(result.length === 0) {
        this.log(method + " > result was empty");
      }
      
      /* always log remote errors */
      if(typeof(result.error) !== "undefined") {
        Logger.log(method + " > error " + result.error.code + ": " + result.error.message);
        return false;
      }
    }
  },
  
  randomString: function() {
    var str = "";
    while (str === "") {
      str = Math.random().toString(36).substr(2, 5);
    }
    return str;
  },
  
  /* logs while this.debug is true */
  log: function(message){
    if(this.debug) {Logger.log(message);}
  },
  
  /* resets the authorization state */
  resetAuth: function() {
    this.oauth.reset();
  },
  
  /* queries for entities by the name of their kind */
  queryByKind: function(name) {
    return this.runQuery({query: {kind:[{name: name}]}});
  },
  
  /* deletes an entity by the name of it's kind and it's id */
  deleteByKindAndId: function(name, id) {
    this.beginTransaction({});
    this.commit({
      "transaction": this.transactionId,
      "mutations": {
        "delete": {
          "partitionId": {"projectId": this.projectId},
          "path": [{"kind": name, "id": id}]
        }
      }
    });
  }
};



/* Test: allocates ids for an entity of kind `strings` (not yet working) */
function allocateIds() {
  var ds = DatastoreApp.getInstance();
  var result = ds.allocateIds({
    "keys": [{
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": "strings"}]
    }]
  });
}

/* Test: allocates ids for an entity of kind `strings` (not yet working) */
function reserveIds() {
  var ds = DatastoreApp.getInstance();
  var result = ds.reserveIds({
    "keys": [{
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": "strings", "id": "6750768661004291"}]
    }, {
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": "strings", "id": "6750768661004292"}]
    }]
  });
  if(typeof(result) !== "undefined") {
    for(i=0; i < result.length; i++) {
      ds.log(JSON.stringify(result[i]));
    }
  }
}

/* Test: looks up entities of kind `strings` */
function lookup() {
  var ds = DatastoreApp.getInstance();
  var result = ds.lookup({
    "keys": [{
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": "strings", "id": TEST_ID}]
    }]
  });
}

/* Test: queries for entities of kind `strings` */
function queryByKind() {
  var ds = DatastoreApp.getInstance();
  var result = ds.queryByKind("strings");
  if(typeof(result.batch) !== "undefined") {
    for(i=0; i < result.batch['entityResults'].length; i++) {
      ds.log(JSON.stringify(result.batch['entityResults'][i]));
    }
  }
}

/* Test: deletes an entity of kind `strings` with id */
function deleteByKindAndId() {
  var ds = gds.getInstance();
  ds.deleteByKindAndId("strings", TEST_ID);
}

/* Test: inserts an entity */
function insertEntity() {

  /* it inserts an entity of kind `strings` with a random string as property `name` */
  var ds = DatastoreApp.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "insert": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": "strings"}]
        },
        "properties":{
          "name": {"stringValue": ds.randomString()}
        }
      }
    }
  });
  
  /* and then rolls back the transaction */
  ds.rollback({
    "transaction": ds.transactionId
  });
}

/* Test: updates an entity */
function updateEntity() {
   
  /* it selects of an entity of kind `strings` by it's id and updates it's property `name` with a random string */
  var ds = DatastoreApp.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "update": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": "strings", "id": TEST_ID}]
        },
        "properties":{
          "name": {"stringValue": ds.randomString()}
        }
      }
    }
  });
}

/* Test: upserts an entity */
function upsertEntity() {

  /* it selects of an entity of kind `strings` by it's id and updates it's property `name` with a random string */
  var ds = DatastoreApp.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "upsert": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": "strings", "id": TEST_ID}]
        },
        "properties":{
          "name": {"stringValue": ds.randomString()}
        }
      }
    }
  });
}
