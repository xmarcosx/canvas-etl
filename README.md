# Canvas ETL via Apache Beam

This repository contains Google Dataflow files designed to batch pull Canvas LMS data via their APi and load into Google BigQuery.

## Google Cloud Platform Configuration
The instructions below assume a Google Cloud Platform (GCP) project has been created and an active billing account attached. They also assume the user is running them in Google's Cloud Shell. Modifications may need to be made if running the commands locally.

Run the commands below to enable the required APIs:
```bash
gcloud services enable dataflow.googleapis.com;
```

Create a Google Cloud Storage bucket that will be used to house the Dataflow template configuration file as well stage various files throughout the ETL process.

```bash
# sets environment variable for your GCP project ID
# skip if running in Google Cloud Shell
GOOGLE_CLOUD_PROJECT=""

# create bucket
gsutil mb -p $GOOGLE_CLOUD_PROJECT gs://$GOOGLE_CLOUD_PROJECT;
```

Create a dataset in BigQuery where the Canvas tables will live:
```bash
bq mk --dataset canvas;
```

### Service Account
Authentication with the GCP project happens through a service account. In GCP, head to _IAM & Admin --> Service Accounts_ to create your service account.

* Click **Create Service Account**
* Choose a name (ie. Canvas ETL) and click **Create**
* Grant the service account the following roles
    * BigQuery Data Editor
    * BigQuery User
    * Cloud Build Service Account
    * Dataflow Developer
    * Dataflow Worker
    * Logging Admin
    * Monitoring Metric Writer
    * Secret Manager Secret Accessor
    * Storage Object Admin
    * Service Account User
* Click **Done**

## Executing locally via DirectRunner
This section explains how to run the Apache Beam scripts locally in a development environment. Clone this repo to your local machine and follow the steps below.

The git repo has a `.devcontainer` folder which supports Visual Studio Code's ability to run all code related to this repo in a Docker container. It's highly recommended this repo be opened in Visual Studio Code in a container.

In GCP, head to IAM & Admin > Service Accounts. Select the actions menu and click **Create key**. Create a JSON key, rename to _service.json_ and store in the root of the `src` directory.

Copy the `.env-sample` file to create your .env file. Complete the variables:

* GOOGLE_APPLICATION_CREDENTIALS: Keep as _service.json_
* PROJECT_ID: This is your GCP project ID
* BUCKET: This is likely _gs://_ followed by your GCP project ID
* REGION: By default this is _us-central1_
* CANVAS_BASE_URL: This is the base URL for the Canvas instance. For example, https://coolschool.instructure.com
* CANVAS_ACCESS_TOKEN: This is the token generated from the Canvas UI.
* SCHOOL_YEAR_START_DATE: This is used to pull all term ids that start on or after this date.

Below are a few sample commands that can be run to test things out.

```bash
cd src;
python ./main.py \
    --endpoint terms \
    --start_date $SCHOOL_YEAR_START_DATE \
    --base_url $CANVAS_BASE_URL \
    --token $CANVAS_ACCESS_TOKEN \
    --project $PROJECT_ID \
    --temp_location "gs://$BUCKET/temp" \
    --runner DirectRunner;
```

## Executing locally to DataflowRunner
Scripts can also be executed from the local development environment, but run on Google's Dataflow.

```bash
cd src;
python ./main.py \
    --endpoint terms \
    --start_date $SCHOOL_YEAR_START_DATE \
    --base_url $CANVAS_BASE_URL \
    --token $CANVAS_ACCESS_TOKEN \
    --project $PROJECT_ID \
    --temp_location "gs://$BUCKET/temp" \
    --runner DataflowRunner \
    --max_num_workers 5 \
    --job_name "canvasterms" \
    --region "us-central1" \
    --setup_file ./setup.py \
    --requirements_file ./job-requirements.txt \
    --experiments enable_prime;
```

## Create template Dataflow jobs
The commands below create a Dataflow template.

```bash
cd src;
GOOGLE_CLOUD_PROJECT="";
REGION="us-central1";
TEMPLATE_IMAGE="gcr.io/$GOOGLE_CLOUD_PROJECT/canvas_etl:latest";
TEMPLATE_PATH="gs://canvas-etl/dataflow/templates/canvas_etl.json";

gcloud config set project $GOOGLE_CLOUD_PROJECT;
gcloud config set builds/use_kaniko True;
gcloud builds submit --tag $TEMPLATE_IMAGE .;
gcloud dataflow flex-template build $TEMPLATE_PATH \
    --image $TEMPLATE_IMAGE \
    --sdk-language "PYTHON" \
    --metadata-file "metadata.json";

# Running Dataflow template

BASE_URL="https://coolschool.instructure.com";
TOKEN="";
ENDPOINT="courses";
SCHOOL_YEAR_START_DATE="2021-09-01";

gcloud beta dataflow flex-template run $ENDPOINT \
    --template-file-gcs-location="$TEMPLATE_PATH" \
    --region="us-central1" \
    --project=$GOOGLE_CLOUD_PROJECT \
    --staging-location="gs://canvas-etl/temp" \
    --max-workers=3 \
    --parameters=endpoint=$ENDPOINT,start_date=$SCHOOL_YEAR_START_DATE,\
base_url=$BASE_URL,token=$TOKEN;
```

## Running Dataflow Template

Head to Dataflow in your Google Cloud project:

1. Click **Create job from template**
2. Select a name (ie. terms)
3. For **Dataflow template** select **Custom template**
4. For **Template path** enter *canvas-etl/dataflow/templates/canvas_etl.json*
5. Enter your Canvas base URL (ie. https://coolschool.instructure.com)
6. Enter an API endpoint from the list below (ie. terms)
7. Enter your school year start date (ie. 2021-09-01)
8. Enter your Canvas API access token
9. Click **Run job**

Run Dataflow jobs in the following order:

1. terms
2. courses
3. in parallel:
    * assignments
    * enrollments
    * sections
4. in parallel:
    * submissions
    * users
