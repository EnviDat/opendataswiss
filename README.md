# OpenDataSwiss

Code to make EnviDat data accessible via OpenDataSwiss scrapers.

- Builds a Python image with all necessary requirements.
- Adds the main.py file to the image.
- Executes main.py at image startup.

## Development

- Build the debug image:
  `docker compose build`

- Create .app.secret:

  ```env
  LOG_LEVEL=DEBUG
  AWS_ENDPOINT=xxx
  AWS_REGION=xxx
  AWS_ACCESS_KEY=xxx
  AWS_SECRET_KEY=xxx
  AWS_BUCKET_NAME=xxx
  BACKEND_PACKAGE_URL=xxx
  ```

- Run via VSCode debug menu.

## Production

- Create the required secrets in the `cron` namespace.
- Push the latest code.
- Watch the build pipeline run in Gitlab.
- Cronjob from the `k8s-cron` repo runs on schedule, using built image.
