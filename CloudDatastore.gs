/*
   Apps Script: Accessing Google Cloud Datastore under a Service Account
   @author Martin Zeitler, https://plus.google.com/106963082057954766426
   @depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   @see https://stackoverflow.com/questions/49112189/49113976#49113976
   @see https://cloud.google.com/datastore/docs/reference/data/rest/
*/

/* Service Account configuration JSON on Google Drive (your filename may vary) */
var JSON_CONFIG = "serviceaccount.json";

/* API wrapper */
var gds = {
  
  debug:       true,
  scopes:      "https://www.googleapis.com/auth/datastore https://www.googleapis.com/auth/drive",
  baseUrl:     "https://datastore.googleapis.com/v1",
  url:         false,
  
  transactionId: false,
  
  projectId:   false,
  clientId:    false,
  clientEmail: false,
  privateKey:  false,
  oauth:       false,
  
  /* returns an instance */
  getInstance: function() {
    
    /* configure the client on demand */
    if(!this.config) {this.getConfig(JSON_CONFIG);}
    
    /* authenticate the client on demand */
    if(!this.oauth) {this.createService();}
    
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
  
  /* projects.operations */
  op: {
    
    /**
     * Starts asynchronous cancellation on a long-running operation.
     * The server makes a best effort to cancel the operation, but success is not guaranteed.
     * If the server doesn't support this method, it returns google.rpc.Code.UNIMPLEMENTED.
     * Clients can use Operations.GetOperation or other methods to check whether the cancellation
     * succeeded or whether the operation completed despite cancellation. On successful cancellation,
     * the operation is not deleted; instead, it becomes an operation with an Operation.error value
     * with a google.rpc.Status.code of 1, corresponding to Code.CANCELLED.
    **/
    cancel: function() {this.request("cancel", false);},
  
    /**
     * Deletes a long-running operation. This method indicates that the client is no longer interested
     * in the operation result. It does not cancel the operation. If the server doesn't support this method,
     * it returns google.rpc.Code.UNIMPLEMENTED.
    **/
    remove: function() {this.request("delete", false);},
    
    /**
     * Gets the latest state of a long-running operation.
     * Clients can use this method to poll the operation result at intervals as recommended by the API service.
    **/
    get: function() {this.request("get", false);},
    
    /**
     * Lists operations that match the specified filter in the request.
     * If the server doesn't support this method, it returns UNIMPLEMENTED.
     * @param payload ~ filter, pageSize, pageToken
    **/
    list: function(payload) {this.request("list", payload);}
  },
  
  /**
   * Queries for entities.
   * @param payload ~ partitionId, readOptions, query, gqlQuery
  **/
  runQuery: function(payload) {this.request("runQuery", payload, false);},
  
  /**
   * Begins a new transaction.
   * @param payload ~ transactionOptions
  **/
  beginTransaction: function(payload) {this.request("beginTransaction", payload, false);},
  
  /**
   * Commits a transaction, optionally creating, deleting or modifying some entities.
   * @param payload ~ mode, mutations, transaction
  **/
  commit: function(payload) {
    this.request("commit", payload, false);
  },
  
  /**
   * Rolls back a transaction.
   * @param payload ~ transaction
  **/
  rollback: function(payload) {this.request("rollback", payload, false);},
  
  /**
   * Allocates IDs for the given keys, which is useful for referencing an entity before it is inserted.
   * @param keys
  **/
  allocateIds: function(keys) {this.request("allocateIds", false, keys);},
  
  /**
   * Prevents the supplied keys' IDs from being auto-allocated by Cloud Datastore.
   * @param keys
  **/
  reserveIds: function(keys) {this.request("reserveIds", false, keys);},
  
  /**
   * Looks up entities by key.
   * @param keys
  **/
  lookup: function(keys) {this.request("lookup", false, keys);},
  
  /* API wrapper for projects */
  request: function(method, payload, keys) {
    
    if (this.oauth.hasAccess()) {
      
      var options = {
        headers: {Authorization: 'Bearer ' + this.oauth.getAccessToken()},
        contentType: "application/json",
        muteHttpExceptions: true
      };
    
      /* the parameters should neither be undefined nor false */
      if(payload !== false) {options.payload = JSON.stringify(payload);}
      if(keys !== false) {options.keys = keys;}
      
      /* the individual api methods are being handled here */
      switch(method) {
        
        case "cancel":
          this.url = this.baseUrl + "/{name=projects/" + this.projectId + "/operations/*}:cancel";
          options.method = "POST";
          break;
          
        case "delete":
          this.url = this.baseUrl + "/{name=projects/" + this.projectId + "/operations/*}";
          options.method = "DELETE";
          break;
        
        case "get":
          this.url = this.baseUrl + "/{name=projects/" + this.projectId + "/operations/*}";
          options.method = "GET";
          break;
        
        case "list":
          this.url = this.baseUrl + "/{name=projects/" + this.projectId + "}/operations";
          options.method = "GET";
          break;
        
        case "runQuery":
          this.url = this.baseUrl + "/projects/" + this.projectId + ":" + method;
          options.method = "POST";
          this.log(method + " > " + options.payload);
          break;
        
        case "beginTransaction":
          this.url = this.baseUrl + "/projects/" + this.projectId + ":" + method;
          options.method = "POST";
          this.log(method + " > " + options.payload);
          break;
        
        case "commit":
          this.url = this.baseUrl + "/projects/" + this.projectId + ":" + method;
          options.method = "POST";
          if(! this.transactionId){
            this.log("cannot commit() while there is no ongoing transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
            this.log(method + " > " + options.payload);
          }
          break;
        
        case "rollback":
          this.url = this.baseUrl + "/projects/" + this.projectId + ":" + method;
          options.method = "POST";
          if(! this.transactionId){
            this.log("cannot rollback() while there is no ongoing transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
            this.log(method + " > " + options.payload);
          }
          break;         
        
        case "allocateIds":
        case "reserveIds":
        case "lookup":
          this.url = this.baseUrl + "/projects/" + this.projectId + ":" + method;
          options.method = "POST";
          this.log(method + " > " + options.keys.join(", "));
          break;
        
        default:
          this.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.url, options);
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
  
  randomString: function() {
    var result = "";
    while (result == "") {result = Math.random().toString(36).substr(2, 5);}
    return result;
  },
  
  /* resets the authorization state */
  resetAuth: function() {
    this.oauth.reset();
  }
};

/* Usage Example */
function run() {

  /* obtain an instance  */
  var ds = gds.getInstance();

  /* it adds an entity of kind `strings` */
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
  
  /* it queries for entities of kind `strings` */
  ds.runQuery({query: {kind:[{name: "strings"}]}});
}
