
python3 scripts/run_bq_terraform.py data/raw/schema.pdf \
    --table events --dataset analytics --project my-gcp-project \
    --timeout 600 --num-ctx 1024

    