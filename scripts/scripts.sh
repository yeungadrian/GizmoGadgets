prefect backend server
prefect server create-tenant -n default
prefect create project teamfighttactics
prefect register -p prefect/users -p prefect/matches --project teamfighttactics
prefect agent local start --env PREFECT__CONTEXT__SECRETS__APIKEY=