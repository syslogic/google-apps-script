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
 
  transactionId: false,
  
  /* returns an instance */
  getInstance: function() {
    
    /* configure the client on demand */
    if(!this.config) {this.getConfig(JSON_CONFIG);}
    
    /* authenticate the client on demand */
    if(!this.oauth) {this.createService();}
    
    return this;
  },
  
  /* loads the config json from Google Drive */
  getConfig: function(filename) {
    var it = DriveApp.getFilesByName(filename);
    while (it.hasNext()) {
      
      var file = it.next();
      var data = JSON.parse(file.getAs("application/json").getDataAsString());
      this.baseUrl     = "https://datastore.googleapis.com/v1/projects/" + data.project_id + ":"
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

  /* API request */
  request: function(method, payload, keys) {
    
    /* the parameters should neither be undefined nor false */
    if(typeof(payload) === "undefined" || !payload) {payload={};}
    if(typeof(keys)    === "undefined" || !keys)    {keys=[];}
    
    if (this.oauth.hasAccess()) {
      
      var options = {
        method: "POST",
        headers: {Authorization: 'Bearer ' + this.oauth.getAccessToken()},
        contentType: "application/json",
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
        keys: keys
      };
      
      /* the individual api methods are being handled here */
      switch(method){
        case "runQuery":
          Logger.log(method + " > " + options.payload);
          break;
          
        case "beginTransaction":
          Logger.log(method + " > " + options.payload);
          break;
          
        case "commit":
          if(! this.transactionId){
            Logger.log("call beginTransaction() before attempting to commit().");
            return false;
          } else {
            payload.transaction = this.transactionId;
            Logger.log(method + " > " + options.payload);
          }
          break;
          
        case "rollback":
          if(! this.transactionId){
            Logger.log("cannot rollback() without a transaction.");
            return false;
          } else {
            payload.transaction = this.transactionId;
            Logger.log(method + " > " + options.payload);
          }
          break;         
          
        case "allocateIds":
        case "reserveIds":
        case "lookup":
          Logger.log(method + " > " + options.keys.join(", "));
          break;
          
        default:
          Logger.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.baseUrl + method, options);
      var result = JSON.parse(response.getContentText());
      this.handleResult(method, result);
      return result;
      
    } else {
      Logger.log(this.oauth.getLastError());
      return false;
    }
  },
    
  /* handles the responses */
  handleResult: function(method, result) {
    
    switch(method){
        
      case "runQuery":
        if(typeof(result.batch) !== "undefined") {
          for(i=0; i < result.batch['entityResults'].length; i++) {
            Logger.log(JSON.stringify(result.batch['entityResults'][i]));
          }
        }
        break;
      
      case "beginTransaction":
        if(typeof(result.transaction) !== "undefined" && result.transaction != "") {
          Logger.log(method + " > " + result.transaction);
          this.transactionId = result.transaction;
        }
        break;
      
      case "commit":
        break;
      
      case "rollback":
        break;
      
      case "allocateIds":
        break;
      
      case "reserveIds":
        break;
      
      case "lookup":
        break;
    }
  },
    
  /* method aliases, which require a payload object */
  runQuery:         function(payload) {this.request("runQuery",         payload, false);},
  beginTransaction: function(payload) {this.request("beginTransaction", payload, false);},
  commit:           function(payload) {this.request("commit",           payload, false);},
  rollback:         function(payload) {this.request("rollback",         payload, false);},
  
  /* method aliases, which require an array of keys */
  allocateIds:      function(keys) {this.request("allocateIds", false, keys);},
  reserveIds:       function(keys) {this.request("reserveIds",  false, keys);},
  lookup:           function(keys) {this.request("lookup",      false, keys);},

  /* resets the authorization state */
  resetAuth: function() {
    this.oauth.reset();
  }
};

/* Cloud Datastore */
function run() {
  
    /* in order not to load the configuration over and over */
    var ds = gds.getInstance();
  
    /* queries the Cloud Datastore */
    ds.runQuery({
      query: {kind:[{name: "strings"}]}
    });
  
    ds.beginTransaction({})
  
    ds.commit({});
}
