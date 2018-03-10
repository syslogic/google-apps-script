/*
   Apps Script: Accessing Google Cloud Datastore under a Service Account.
   @author Martin Zeitler, https://plus.google.com/106963082057954766426
   @bitcoin 19uySyXrtqQ71PFZWHb2PxBwtNitg2Dp6b

  Service Account configuration file on Google Drive
  (the "Cloud Datastore User" role must be assigned).
*/
var CONFIG = "serviceaccount.json";

/* Kind, ID and Name of an Entity, which is used by below functional tests */
var test = {KIND: "strings", ID: "5558520099373056", NAME: "2ja7h"};

/* API wrapper */
var datastore = {
  
  /* verbose logging */
  debug:         true,
  
  /* api related */
  scopes:        "https://www.googleapis.com/auth/datastore https://www.googleapis.com/auth/drive",
  baseUrl:       "https://datastore.googleapis.com/v1",
  httpMethod:    "POST",
  currentUrl:    false,
  
  /* authentication */
  oauth:         false,
  projectId:     false,
  clientId:      false,
  clientEmail:   false,
  privateKey:    false,
  
  /* transactions */
  transactionId: false,
  
  /* pagination */
  startCursor:   false,
  queryInProgress: false,
  currentPage:   1,
  totalPages:    1,
  perPage:       5,
  
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

    /* wait for the previous request to complete, check every 200 ms */
    while(this.queryInProgress) {Utilities.sleep(200);}
    
    if (this.oauth.hasAccess()) {

      /* set queryInProgress flag to true */
      this.queryInProgress = true;
      
      /* configure the request */
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
        
        /* result.query */
        if(typeof(result.query) !== "undefined") {
          for(i=0; i < result.query.length; i++) {
            this.log(JSON.stringify(result.query[i]));
          }
        }
        
        /* result.batch */
        if(typeof(result.batch) !== "undefined") {
          
          /* log the entityResults */
          for(i=0; i < result.batch["entityResults"].length; i++) {
            this.log(JSON.stringify(result.batch['entityResults'][i]));
          }
          
          /* set the endCursor as the next one startCursor */
          if(typeof(result.batch["moreResults"]) !== "undefined") {
            
            switch(result.batch["moreResults"]) {
                
              /* There may be additional batches to fetch from this query. */
              case "NOT_FINISHED":
                
              /* The query is finished, but there may be more results after the limit. */
              case "MORE_RESULTS_AFTER_LIMIT":
              
              /* The query is finished, but there may be more results after the end cursor. */
              case "MORE_RESULTS_AFTER_CURSOR":
                this.startCursor = result.batch["endCursor"];
                break;
              
              /* The query is finished, and there are no more results. */
              case "NO_MORE_RESULTS":
                this.startCursor = false;
                break;
            }

          } else {
            this.startCursor = false;
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

          /* resetting the transaction in progress */
          this.transactionId = false;
          
        } else {
          
          /* log the mutationResults */
          if(typeof(result.mutationResults) !== "undefined") {
            for(i=0; i < result.mutationResults.length; i++) {
              this.log(JSON.stringify(result.mutationResults[i]));
            }
          }
        }
        break;
      
      /* projects.rollback */
      case "rollback":
        /* resetting the transaction in progress */
        this.transactionId = false;
        break;
      
      /* projects.allocateIds */
      case "allocateIds":
        
        /* log allocated keys */
        if(typeof(result.keys) !== "undefined") {
            for(i=0; i < result.keys.length; i++) {
              this.log(JSON.stringify(result.keys[i]));
            }
        }
        break;
      
      /* projects.reserveIds (the response is empty by default) */
      case "reserveIds":
        break;
      
      /* projects.lookup */
      case "lookup":
        
        /* log found entities */
        if(typeof(result.found) !== "undefined") {
            for(i=0; i < result.found.length; i++) {
              this.log(JSON.stringify(result.found[i]));
            }
        }
        
        /* log missing entities */
        if(typeof(result.missing) !== "undefined") {
          for(i=0; i < result.missing.length; i++) {
            this.log(JSON.stringify(result.missing[i]));
          }
        }
        
        /* log deferred entities */
        if(typeof(result.deferred) !== "undefined") {
          for(i=0; i < result.deferred.length; i++) {
            this.log(JSON.stringify(result.deferred[i]));
          }
        }
        break;
    }
    
    /* set queryInProgress flag to false */
    this.queryInProgress = false;
    
    /* always log remote errors */
    if(typeof(result.error) !== "undefined") {
      Logger.log(method + " > ERROR " + result.error.code + ": " + result.error.message);
      return false;
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
  
  /**
   * runs a simple GQL query
   * @see https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#GqlQuery
  **/
  runGql: function(query_string) {
    if(! this.startCursor) {
      var options = {
        gqlQuery: {
          query_string: query_string,
          allowLiterals: true
        }
      };
    } else {
      var options = {
        gqlQuery: {
          query_string: query_string,
          allowLiterals: true,
          namedBindings: {
            startCursor: {cursor: this.startCursor}
          }
        }
      };
    }
    this.log(JSON.stringify(options));
    return this.runQuery(options);
  },
  
  /* queries for entities by the name of their kind */
  queryByKind: function(value) {
    return this.runQuery({query: {kind:[{name: value}]}});
  },
  
  lookupById: function(value) {
    return this.lookup({
      "keys": [{
        "partitionId": {"projectId": this.projectId},
        "path": [{"kind": "strings", "id": value}]
      }]
    });
  },
  
  lookupByName: function(value) {
    return this.lookup({
      "keys": [{
        "partitionId": {"projectId": this.projectId},
        "path": [{"kind": "strings", "name": value}]
      }]
    });
  },
  
  /* deletes an entity by the name of it's kind and it's id */
  deleteByKindAndId: function(value, id) {
    this.beginTransaction({});
    return this.commit({
      "transaction": this.transactionId,
      "mutations": {
        "delete": {
          "partitionId": {"projectId": this.projectId},
          "path": [{"kind": value, "id": id}]
        }
      }
    });
  }
};


/* Test: looks up entities of kind `strings` with id TEST_ID */
function lookupById() {
  var ds = datastore.getInstance();
  var result = ds.lookupById(test.ID);
}

/* Test: looks up entities of kind `strings` with name TEST_NAME */
function lookupByName() {
  var ds = datastore.getInstance();
  var result = ds.lookupByName(test.NAME);
}

/* Test: queries for entities of kind `strings` */
function queryByKind() {
  var ds = datastore.getInstance();
  var result = ds.queryByKind(test.KIND);
}

/* Test: run a GQL query */
function queryByKindPaged() {
  var ds = datastore.getInstance();
  var i=0;
  
  /* run the first query, yielding a startCursor */
  var result = ds.runGql("SELECT * FROM " + test.KIND + " ORDER BY name ASC LIMIT " + ds.perPage + " OFFSET 0");
  
  /* when the startCursor is false,the last page had been reached */
  while(ds.startCursor && i < 3){
    var result = ds.runGql("SELECT * FROM " + test.KIND + " ORDER BY name ASC LIMIT " + ds.perPage + " OFFSET @startCursor");
    i++;
  }
}

/* Test: deletes an entity of kind `strings` with id */
function deleteByKindAndId() {
  var ds = datastore.getInstance();
  ds.deleteByKindAndId("strings", test.ID);
}

/* Test: inserts an entity */
function insertEntity() {

  /* it inserts an entity of kind `strings` with a random string as property `name` */
  var ds = datastore.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "insert": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": test.KIND}]
        },
        "properties":{
          "name": {"stringValue": ds.randomString()}
        }
      }
    }
  });
}

/* Test: updates an entity */
function updateEntity() {
   
  /* it selects of an entity of kind `strings` by it's id and updates it's property `name` with a random string */
  var ds = datastore.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "update": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": test.KIND, "id": test.ID}]
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
  var ds = datastore.getInstance();
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "upsert": {
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": test.KIND, "id": test.ID}]
        },
        "properties":{
          "name": {"stringValue": ds.randomString()}
          
        }
      }
    }
  });
}

/* Test: allocates ids for entities of kind `strings` */
function allocateIds() {
  var ds = datastore.getInstance();
  var result = ds.allocateIds({
    "keys": [{
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": test.KIND}]
    }]
  });
}

/* Test: reserves ids for entities of kind `strings` */
function reserveIds() {
  var ds = datastore.getInstance();
  ds.reserveIds({
    "keys": [{
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": test.KIND, "id": "6750768661004291"}]
    }, {
      "partitionId": {"projectId": ds.projectId},
      "path": [{"kind": test.KIND, "id": "6750768661004292"}]
    }]
  });
}
