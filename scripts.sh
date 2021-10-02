prefect backend server
prefect server create-tenant -n default
prefect create project teamfighttactics
prefect register -p prefect --project teamfighttactics
prefect agent local starts