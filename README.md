# veryfi 

This project simulates a receipt scanning system and uses PostgreSQL, Apache Airflow, Docker, and FastAPI.

Before executing the project, make sure to check if `docker` and `docker compose` is present in your system. You can do this by executing

```
docker --version
docker-compose --version
```

For instance, in my machine this returns
```                
Docker version 20.10.23, build 7155243
Docker Compose version v2.15.1
```

Once you have performed the check and ensured that you have the needed requirements. Begin executing the project, type in the following commands from the terminal after cloning the GitHub project.

```
chmod +x setup.sh
bash setup.h
```

This will prune the system of existing docker images, create folders `./plugins` and `./logs` for Airflow, and begin `docker compose`.

Note: `Compose` is a tool for defining and running multi-container Docker applications. A YAML file is used to configure your application's services. Here, we use `Compose` here.

If you wish to stop the process, press `Control + C` from your keyboard and then clear the process using.
```
docker compose down --volumes --remove-orphans
```