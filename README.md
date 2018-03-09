# CloudDatastore.gs
CloudDatastore.gs is a client for Google Cloud Datastore, which runs as a Service Account.

It is written in Apps Script. and so far, it can select, insert, update and delete entities.

## Setup

a) Add the script to https://script.google.com

b) Add the `OAuth2 for Apps Script` library: `1B7FSrk5Zi6L1rSxxTDgDEUsPzlukDsi4KGuTMorsTQHhGBzBkMun4iDF`.

c) Upload the service account configuration file to your Google Drive (to be downloaded from https://console.cloud.google.com/project/_/iam-admin and then uploaded to https://drive.google.com).

The service account needs to have the "Cloud Datatore User" role asigned.

d) Adjust the script according to the filename of the configuration file.

## Reference

@see https://github.com/googlesamples/apps-script-oauth2

@see https://stackoverflow.com/questions/49112189/49113976#49113976

@see https://cloud.google.com/datastore/docs/reference/data/rest/
