/*
   Apps Script: Accessing the Google Cloud Datastore under a Service Account
   Author: Martin Zeitler, https://plus.google.com/106963082057954766426
   depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   see: https://stackoverflow.com/questions/49112189/49113976#49113976
   and https://cloud.google.com/datastore/docs/reference/data/rest/
*/

var PROJECT_ID   = "...";
var CLIENT_ID    = "...";
var CLIENT_EMAIL = "...-compute@developer.gserviceaccount.com";
var PRIVATE_KEY  = "...";
var SCOPE        = "https://www.googleapis.com/auth/datastore";
var BASEURL      = "https://datastore.googleapis.com";

/* API wrapper */
var gds = {
  
  baseUrl: BASEURL + "/v1/projects/" + PROJECT_ID + ":",
  transactionId: false,
  oauth: false,

  /* it creates the oAuth2 service */
  createOAuth2Service: function() {
    this.oauth = OAuth2.createService("Datastore")
    .setTokenUrl("https://www.googleapis.com/oauth2/v4/token")
    .setPropertyStore(PropertiesService.getScriptProperties())
    // .setSubject(Session.getActiveUser().getEmail())
    .setPrivateKey(PRIVATE_KEY)
    .setIssuer(CLIENT_EMAIL)
    .setScope(SCOPE);
  },

  /* API request */
  request: function(method, payload, keys) {
    
    /* the parameters should neither be undefined nor false */
    if(typeof(payload) === "undefined" || !payload) {payload={};}
    if(typeof(keys)    === "undefined" || !keys)    {keys=[];}
    
    /* authenticate the client on demand */
    if(!this.oauth) {this.createOAuth2Service();}
    
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
          
        case "allocateIds":
          Logger.log(method + " > " + options.keys.join(", "));
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
          
        case "lookup":
          Logger.log(method + " > " + options.keys.join(", "));
          break;
          
        case "reserveIds":
          Logger.log(method + " > " + options.payload);
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
          
        default:
          Logger.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.baseUrl + method, options);
      var result = JSON.parse(response.getContentText());
      this.handleResult(method, result);
      return result;
      
    } else {
      Logger.log(this.service.getLastError());
      return false;
    }
  },
    
  /* the responses are being handled here */
  handleResult: function(method, result) {
    
    switch(method){
        
      case "runQuery":
        for(i=0; i < result.batch['entityResults'].length; i++) {
          Logger.log(JSON.stringify(result.batch['entityResults'][i]));
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

/* it queries the Cloud Datastore */
function run() {
    gds.runQuery({
      query: {kind:[{name: "strings"}]}
    });
}
