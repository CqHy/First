// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.hystorev2.dfs.core.windows.net",
  dbutils.secrets.get(scope = "storeV2", key = "storev2key"))


// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "<application-id>",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "", key = "<key-name-for-service-credential>"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/<directory-id>/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
  mountPoint = "/mnt/<mount-name>",
  extraConfigs = configs)