curl --location --request POST 'https://sales-exp-api.us-e2.cloudhub.io/api/v1/ext-availability' \
--header 'ext_client_id: 992ea300a90c4b66bc713dae33384bde' \
--header 'ext_client_secret: 9Cd55E96F36e4032A31BdFAfEF426f11' \
--header 'Content-Type: application/json' \
--data-raw '{
  "start_date": "2025-10-21",
  "end_date": "2025-11-18",
  "display_ids": "1541445,1541852"
}'



curl --location --request POST 'https://sales-exp-api.us-e2.cloudhub.io/api/v1/ext-availability' \
--header 'ext_client_id: 992ea300a90c4b66bc713dae33384bde' \
--header 'ext_client_secret: 9Cd55E96F36e4032A31BdFAfEF426f11' \
--header 'Content-Type: application/json' \
--data-raw '{
  "start_date": "2025-10-24",
  "end_date": "2025-10-31",
  "geopath_ids": "259451,25945"
}'

