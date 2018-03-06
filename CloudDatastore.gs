/*
   Apps Script: Accessing the Google Cloud Datastore under a Service Account
   depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   see: https://stackoverflow.com/questions/49112189/49113976#49113976
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

  /* API request, see https://cloud.google.com/datastore/docs/reference/data/rest/ */
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
      
      /* the individual api methods can be handled here */
      switch(method){
        case "runQuery":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "allocateIds":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "beginTransaction":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "commit":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "lookup":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "reserveIds":
          Logger.log(method + " >> " + options.payload);
          break;
          
        case "rollback":
          Logger.log(method + " >> " + options.payload);
          break;
          
        default:
          Logger.log("invalid api method: "+ method);
          return false;
      }
      
      var response = UrlFetchApp.fetch(this.baseUrl + method, options);
      var result = JSON.parse(response.getContentText());
      
      for(i=0; i < result.batch['entityResults'].length; i++){
        Logger.log(JSON.stringify(result.batch['entityResults'][i]));
      }
      return result;
      
    } else {
      Logger.log(this.service.getLastError());
      return false;
    }
  },
  
  /* method wrappers, for calling the API methods by their name */
  runQuery:         function(payload, keys) {        this.request("runQuery", payload, keys);},
  allocateIds:      function(payload, keys) {     this.request("allocateIds", payload, keys);},
  beginTransaction: function(payload, keys) {this.request("beginTransaction", payload, keys);},
  commit:           function(payload, keys) {          this.request("commit", payload, keys);},
  lookup:           function(payload, keys) {          this.request("lookup", payload, keys);},
  reserveIds:       function(payload, keys) {      this.request("reserveIds", payload, keys);},
  rollback:         function(payload, keys) {        this.request("rollback", payload, keys);},
  
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
