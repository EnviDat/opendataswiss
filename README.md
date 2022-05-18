# OpenDataSwiss

Code to make EnviDat data accessible via OpenDataSwiss scrapers.

- Builds a Python image with all necessary requirements.
- Adds the main.py file to the image.
- Executes main.py at image startup.

## Development

1. Create .env.secret:

```env
LOG_LEVEL=DEBUG
API_URL=xxx
AWS_ENDPOINT=xxx
AWS_REGION=xxx
AWS_ACCESS_KEY=xxx
AWS_SECRET_KEY=xxx
```

2a. Local Debug

- Install `pdm` and enable pep582 support `pdm --pep582`.
- Install project dependencies `pdm install`
- Run with IDE debugger

2b. Remote Debug

- Build the debug image:
  `docker compose build`
- Start the container:
  `docker compose up -d`
- Run remote debugging via IDE (VSCode) debug menu.

## Production

- Create the required secrets in the `cron` namespace.
- Push the latest code.
- Watch the build pipeline run in Gitlab.
- Cronjob from the `k8s-cron` repo runs on schedule, using built image.
