prefect backend server
prefect server create-tenant -n default
prefect create project teamfighttactics
prefect register -p prefect --project teamfighttactics
prefect agent local start --env PREFECT__CONTEXT__SECRETS__APIKEY=RGAPI-5c85443b-0bc0-4263-b969-286f84a34b05