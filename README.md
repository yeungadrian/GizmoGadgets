# ETL Riot Games
Using Prefect & Python to load data from the Riot Games API into a postgresql database. Allows people to get match history data to analyse the best item / team composition or build profiles of users

## Requirements:
- Python
- Prefect
- Docker
- Postgresql
- pgAdmin

## Installation
Setup a python virtual environment with https://docs.python.org/3/library/venv.html
```html
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Easy local setup using docker-compose:
```html
docker-compose up
```
Scripts to setup Prefect locally contained in scripts.sh

## Database
Designed in https://dbdiagram.io with easy export to postgresql functionality
![](images/dbdiagram.png)

pgAdmin available at http://localhost:5050 to view/edit database
![](images/pgAdmin.png)

## Prefect Server
Use Prefect Server at http://localhost:8080 to monitor and execute flows
![](images/prefect.png)
