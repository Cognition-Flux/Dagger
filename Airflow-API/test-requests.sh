ENDPOINT_URL="http://localhost:8080/"
curl -X GET  \
    --user "airflow:airflow" \
    "${ENDPOINT_URL}/api/v1/dags"

ENDPOINT_URL="http://54.227.67.9:8080/"
curl -X GET  \
    --user "dag-test:dag-test" \
    "${ENDPOINT_URL}/api/v1/dags"



ENDPOINT_URL="http://airflow-alb-1817038188.us-east-1.elb.amazonaws.com/"
curl -X GET  \
    --user "dag-test:dag-test" \
    "${ENDPOINT_URL}/api/v1/dags"