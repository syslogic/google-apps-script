/*
   Apps Script: Accessing the Google Cloud Datastore under a Service Account
   depends on `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`
   see: https://stackoverflow.com/questions/49112189/49113976#49113976
*/

var projectId = "";

var PRIVATE_KEY = "";
var CLIENT_EMAIL = "";
var CLIENT_ID = "";
var USER_EMAIL = "";

var BASEURL = "https://datastore.googleapis.com";
var SCOPE = "https://www.googleapis.com/auth/datastore";


/* create the service */
var service = OAuth2.createService("Datastore")
.setTokenUrl('https://www.googleapis.com/oauth2/v4/token')
.setPropertyStore(PropertiesService.getScriptProperties())
// .setSubject(Session.getActiveUser().getEmail())
.setPrivateKey(PRIVATE_KEY)
.setIssuer(CLIENT_EMAIL)
.setScope(SCOPES);

/* make a request to the Cloud Datastore API. */
function run() {
  if (service.hasAccess()) {

    var response = runQuery({
      query: {kind:[{name: "strings"}]}
    });

    Logger.log(JSON.stringify(response, null, 2));
  } else {
    Logger.log(service.getLastError());
  }
}

/* api wrapper methods */
function request(method, payload) {
  if (service.hasAccess()) {

    var options = {
      method: "POST",
      headers: {Authorization: 'Bearer ' + service.getAccessToken()},
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      keys: []
    };

    switch(method){
      case "runQuery": case "allocateIds": case "beginTransaction": case "commit": case "lookup": case "reserveIds": case "rollback": break;
      default: Logger.log("invalid api method: "+ method); return false;
    }
    var response = UrlFetchApp.fetch(BASEURL + "/v1/projects/" + projectId + ":" + method, options);
    return JSON.parse(response.getContentText());
  }
}

/* aliases for function calls */
function runQuery(payload)         {request("runQuery", payload);}
function allocateIds(payload)      {request("allocateIds", payload);}
function beginTransaction(payload) {request("beginTransaction", payload);}
function commit(payload)           {request("commit", payload);}
function lookup(payload)           {request("lookup", payload);}
function reserveIds(payload)       {request("reserveIds", payload);}
function rollback(payload)         {request("rollback", payload);}

/* resets the authorization state, so that it can be re-tested. */
function reset() {
  service.reset();
}
