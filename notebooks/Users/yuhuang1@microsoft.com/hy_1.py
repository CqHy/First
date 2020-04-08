# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/postgresql-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/postgresql-42.2.2.jar https://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar
wget --quiet -O /mnt/jars/driver-daemon/postgresql-42.2.2.jar https://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar""", True)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks/scripts/postgresql-install.sh"))

# COMMAND ----------

script = """#!/bin/bash

set -euxo pipefail

if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  apt-get update
  apt-get install -y gdebi-core
  cd /tmp
  # You can find new releases at https://rstudio.com/products/rstudio/download-server/debian-ubuntu/.
  wget https://download2.rstudio.org/server/trusty/amd64/rstudio-server-1.2.5001-amd64.deb
  sudo gdebi -n rstudio-server-1.2.5001-amd64.deb
  rstudio-server restart || true
fi
"""

dbutils.fs.mkdirs("/databricks/rstudio")
dbutils.fs.put("/databricks/rstudio/rstudio-install.sh", script, True)

# COMMAND ----------

script = """#!/bin/bash
set -euxo pipefail
if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  apt-get update
  apt-get install -y gdebi-core
  cd /tmp
  # You can find new releases at https://rstudio.com/products/rstudio/download-server/debian-ubuntu/.
  wget https://download2.rstudio.org/server/trusty/amd64/rstudio-server-1.2.5033-amd64.deb
 sudo gdebi -n rstudio-server-1.2.5033-amd64.deb
  rstudio-server restart || true
fi
"""

dbutils.fs.put("/databricks/rstudio/rstudio-install1.sh", script, True)

# COMMAND ----------

#github test
