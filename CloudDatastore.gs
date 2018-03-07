/*
   Apps Script: Accessing the Google Cloud Datastore under a Service Account
   @author: Martin Zeitler, https://plus.google.com/106963082057954766426
   @depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   @see https://stackoverflow.com/questions/49112189/49113976#49113976
   @see https://cloud.google.com/datastore/docs/reference/data/rest/
*/

/* Service Account configuration JSON on Google Drive (your filename may vary) */
var JSON_CONFIG = "serviceaccount.json";

/* API wrapper */
var gds = {
  
  scopes: "https://www.googleapis.com/auth/datastore https://www.googleapis.com/auth/drive",

  projectId:   false,
  baseUrl:     false,
  clientId:    false,
  clientEmail: false,
  privateKey:  false,
  oauth:       false,
  debug:       true,
  
  transactionId: false,
  
  /* returns an instance */
  getInstance: function() {
    
    /* configure the client on demand */
    if(!this.config) {this.getConfig(JSON_CONFIG);}
    
    /* authenticate the client on demand */
    if(!this.oauth) {this.createService();}
    
    return this;
  },
  
  /**
   * projects.operations.cancel
   * Starts asynchronous cancellation on a long-running operation. The server makes a best effort to cancel the operation, but success is not guaranteed.
   * If the server doesn't support this method, it returns google.rpc.Code.UNIMPLEMENTED. Clients can use Operations.GetOperation or other methods to check
   * whether the cancellation succeeded or whether the operation completed despite cancellation. On successful cancellation, the operation is not deleted;
   * instead, it becomes an operation with an Operation.error value with a google.rpc.Status.code of 1, corresponding to Code.CANCELLED.
  **/
  cancel: function() {this.operations("cancel", false);},
  
  /**
   * projects.operations.delete
   * Deletes a long-running operation. This method indicates that the client is no longer interested in the operation result.
   * It does not cancel the operation. If the server doesn't support this method, it returns google.rpc.Code.UNIMPLEMENTED.
  **/
  remove: function() {this.operations("delete", false);},
  
  /**
   * projects.operations.get
   * Gets the latest state of a long-running operation.
   * Clients can use this method to poll the operation result at intervals as recommended by the API service.
  **/
  get: function() {this.operations("get", false);},
  
  /**
   * projects.operations.list
   * Lists operations that match the specified filter in the request. If the server doesn't support this method, it returns UNIMPLEMENTED.
   * @param payload ~ filter, pageSize, pageToken
  **/
  list: function(payload) {this.operations("list", payload);},
  
  /**
   * projects.runQuery
   * Queries for entities.
   * @param payload ~ partitionId, readOptions, query, gqlQuery
  **/
  runQuery: function(payload) {this.request("runQuery", payload, false);},
  
  /**
   * projects.beginTransaction
   * Begins a new transaction.
   * @param payload ~ transactionOptions
  **/
  beginTransaction: function(payload) {this.request("beginTransaction", payload, false);},
  
  /**
   * projects.commit
   * Commits a transaction, optionally creating, deleting or modifying some entities.
   * @param payload ~ mode, mutations, transaction
  **/
  commit: function(payload) {
    this.request("commit", payload, false);
  },
  
  /**
   * projects.rollback
   * Rolls back a transaction.
   * @param payload ~ transaction
  **/
  rollback: function(payload) {this.request("rollback", payload, false);},
  
  /**
   * projects.allocateIds
   * Allocates IDs for the given keys, which is useful for referencing an entity before it is inserted.
   * @param keys
  **/
  allocateIds: function(keys) {this.request("allocateIds", false, keys);},
  
  /**
   * projects.reserveIds
   * Prevents the supplied keys' IDs from being auto-allocated by Cloud Datastore.
   * @param keys
  **/
  reserveIds: function(keys) {this.request("reserveIds", false, keys);},
  
  /**
   * projects.lookup
   * Looks up entities by key.
   * @param keys
  **/
  lookup: function(keys) {this.request("lookup", false, keys);},
  
  /* loads the config json from Google Drive */
  getConfig: function(filename) {
    var it = DriveApp.getFilesByName(filename);
    while (it.hasNext()) {
      var file = it.next();
      var data = JSON.parse(file.getAs("application/json").getDataAsString());
      this.baseUrl     = "https://datastore.googleapis.com/v1/projects/" + data.project_id + ":";
      this.baseUrlOp   = "https://datastore.googleapis.com/v1/{name=projects/*}/operations";
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

  
  /* API wrapper, GET projects.operations */
  operation: function(method, payload) {
    
    if (this.oauth.hasAccess()) {
    
      var options = {
        method: "GET",
        headers: {Authorization: 'Bearer ' + this.oauth.getAccessToken()},
        contentType: "application/json",
        muteHttpExceptions: true
      };
      
      /* the individual api methods are being handled here */
      switch(method){
        case "cancel":
          this.log(method + " > ");
          break;
        case "delete":
          this.log(method + " > ");
          break;
        case "get":
          this.log(method + " > ");
          break;
        case "list":
          this.log(method + " > ");
          break;
        default:
          this.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.baseUrlOp, options);
      var result = JSON.parse(response.getContentText());
      this.handleResult(method, result);
      return result;
      
    } else {
      this.log(this.oauth.getLastError());
      return false;
    }
  },
  
  /* API wrapper, POST projects */
  request: function(method, payload, keys) {
    
    if (this.oauth.hasAccess()) {
      
      var options = {
        method: "POST",
        headers: {Authorization: 'Bearer ' + this.oauth.getAccessToken()},
        contentType: "application/json",
        muteHttpExceptions: true
      };
    
      /* the parameters should neither be undefined nor false */
      if(payload !== false) {options.payload = JSON.stringify(payload);}
      if(keys !== false) {options.keys = keys;}
      
      /* the individual api methods are being handled here */
      switch(method){
        case "runQuery":
          this.log(method + " > " + options.payload);
          break;
          
        case "beginTransaction":
          this.log(method + " > " + options.payload);
          break;
          
        case "commit":
          if(! this.transactionId){
            this.log("call beginTransaction() before attempting to commit().");
            return false;
          } else {
            payload.transaction = this.transactionId;
            this.log(method + " > " + options.payload);
          }
          break;
          
        case "rollback":
          if(! this.transactionId){
            this.log("cannot rollback() without a transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
            this.log(method + " > " + options.payload);
          }
          break;         
        
        case "allocateIds":
        case "reserveIds":
        case "lookup":
          this.log(method + " > " + options.keys.join(", "));
          break;
          
        default:
          this.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.baseUrl + method, options);
      var result = JSON.parse(response.getContentText());
      this.handleResult(method, result);
      return result;
      
    } else {
      this.log(this.oauth.getLastError());
      return false;
    }
  },
  
  /* handles the responses */
  handleResult: function(method, result) {
    
    switch(method){
        
      case "runQuery":
        if(typeof(result.batch) !== "undefined") {
          for(i=0; i < result.batch['entityResults'].length; i++) {
            this.log(JSON.stringify(result.batch['entityResults'][i]));
          }
        }
        break;
      
      case "beginTransaction":
        if(typeof(result.transaction) !== "undefined" && result.transaction != "") {
          this.log(method + " > " + result.transaction);
          this.transactionId = result.transaction;
        }
        break;
      
      case "commit":
        if(typeof(result.error) !== "undefined") {
          this.log(method + " > error " + result.error.code + ": " + result.error.message);
          this.transactionId = false;
        } else {
          if(typeof(result.mutationResults) !== "undefined") {
            for(i=0; i < result.mutationResults.length; i++) {
              this.log(JSON.stringify(result.mutationResults[i]));
            }
          }
          if(typeof(result.commitVersion) !== "undefined") {
            this.log("commitVersion" + result.commitVersion);
          }
        }
        break;
      
      case "rollback":
        break;
      
      case "allocateIds":
        break;
      
      case "reserveIds":
        break;
      
      case "lookup":
        break;
      
      case "cancel":
        break;
        
      case "delete":
        break;
      
      case "get":
        break;
      
      case "list":
        break;
    }
  },
  
  /* logs while this.debug is true */
  log: function(message){
    if(this.debug) {Logger.log(message);}
  },
  
  /* resets the authorization state */
  resetAuth: function() {
    this.oauth.reset();
  }
};

/* Cloud Datastore */
function run() {

  /* in order not to load the configuration over and over */
  var ds = gds.getInstance();

  /* adds an entity of kind `strings` */
  ds.beginTransaction({});
  ds.commit({
    "transaction": ds.transactionId,
    "mutations": {
      "insert": {
        // "properties": [{name: 'name', value: "asdf"}],
        "key": {
          "partitionId": {"projectId": ds.projectId},
          "path": [{"kind": "strings"}]
        } 
      }
    }
  });
  
  /* queries for entities of kind `strings` */
  ds.runQuery({query: {kind:[{name: "strings"}]}});
}
