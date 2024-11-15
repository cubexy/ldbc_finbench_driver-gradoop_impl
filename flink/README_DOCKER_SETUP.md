# Starte eine interaktive Bash-Shell im hdfs-namenode-Container
docker exec -it hdfs-namenode bash
# Gebe jedem im Root-Directory Schreibzugriff (NICHT EMPFOHLEN! Nur für lokale Entwicklung.)
hdfs dfs -chmod 777 /