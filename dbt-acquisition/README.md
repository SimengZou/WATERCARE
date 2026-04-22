# DataEngineering

## Model Automation

The `entrypoint.py` script automates the generation of dbt schemas and configured `__sources.yml`/`.sql` models directly from the Infor API.

### Environment Setup

You must configure a `.env` file in the root directory containing your specific API credentials:

```env
LOG_LEVEL=INFO
INFOR_API_ENVIRONMENT=
INFOR_API_AUTH_URL=
INFOR_API_DATA_URL=
INFOR_API_CLIENT_ID=
INFOR_API_CLIENT_SECRET=
INFOR_API_USERNAME=
INFOR_API_PASSWORD=
SNOWFLAKE_USER=
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_ROLE=
```

### Execution

Setup and execution of dbt model generation:
```bash
pip install -r requirements.txt
py entrypoint.py --help
```
Example:
```bash
py entrypoint.py --download "^(IPS|DEPM|EAM|LN)_" --no-generate
```
