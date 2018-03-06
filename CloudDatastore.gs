/*
   Apps Script: Accessing the Google Cloud Datastore under a Service Account
   depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   see: https://stackoverflow.com/questions/49112189/49113976#49113976
*/

var PROJECT_ID   = "";
var CLIENT_ID    = "";
var CLIENT_EMAIL = "...-compute@developer.gserviceaccount.com";
var PRIVATE_KEY  = "";
var SCOPE        = "https://www.googleapis.com/auth/datastore";
var BASEURL      = "https://datastore.googleapis.com";

/* API wrapper */
var gds = {
  
  service: false,

  createService: function() {
    this.service = OAuth2.createService("Datastore")
    .setTokenUrl('https://www.googleapis.com/oauth2/v4/token')
    .setPropertyStore(PropertiesService.getScriptProperties())
    // .setSubject(Session.getActiveUser().getEmail())
    .setPrivateKey(PRIVATE_KEY)
    .setIssuer(CLIENT_EMAIL)
    .setScope(SCOPE);
  },
  
  request: function(method, payload, keys) {
    if(!this.service){this.createService();}
    if (this.service.hasAccess()) {
      
      if(typeof(payload) === "undefined"){payload={};}
      if(typeof(keys) === "undefined"){keys=[];}
      
      var options = {
        method: "POST",
        headers: {Authorization: 'Bearer ' + this.service.getAccessToken()},
        contentType: "application/json",
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
        keys: keys
      };
      
      switch(method){
        case "runQuery": case "allocateIds": case "beginTransaction": case "commit": case "lookup": case "reserveIds": case "rollback": break;
        default: Logger.log("invalid api method: "+ method); return false;
      }
      var response = UrlFetchApp.fetch(BASEURL + "/v1/projects/" + PROJECT_ID + ":" + method, options);
      var result = JSON.parse(response.getContentText());
      var headers = response.getHeaders();
      
      for(i=0; i < result.batch['entityResults'].length; i++){
        Logger.log(JSON.stringify(result.batch['entityResults'][i]));
      }
      return result;
      
    } else {
      Logger.log(this.service.getLastError());
      return false;
    }
  },
  
  /* aliases for function calls */
  runQuery:         function(payload, keys) {        this.request("runQuery", payload, keys);},
  allocateIds:      function(payload, keys) {     this.request("allocateIds", payload, keys);},
  beginTransaction: function(payload, keys) {this.request("beginTransaction", payload, keys);},
  commit:           function(payload, keys) {          this.request("commit", payload, keys);},
  lookup:           function(payload, keys) {          this.request("lookup", payload, keys);},
  reserveIds:       function(payload, keys) {      this.request("reserveIds", payload, keys);},
  rollback:         function(payload, keys) {        this.request("rollback", payload, keys);},
  
  /* resets the authorization state */
  resetAuth: function() {
    this.service.reset();
  }
};

/* makes a request to the Cloud Datastore API. */
function run() {
  
    gds.runQuery({
      query: {kind:[{name: "strings"}]}
    });
}
