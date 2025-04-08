
from google.cloud import bigquery

def run_bq_update():
    client = bigquery.Client(project="practicebigdataanalytics")
    query = """
    UPDATE `practicebigdataanalytics.sensor_dataset.aggregated_sensor_data`
    SET 
      promo_code = IF(promo_code IS NULL OR TRIM(promo_code) = '', 'invalid', promo_code),
      event_id = IF(event_id IS NULL OR TRIM(event_id) = '', 'invalid', event_id),
      user_id = IF(user_id IS NULL OR TRIM(user_id) = '', 'invalid', user_id),
      attributes_user_id = IF(attributes_user_id IS NULL OR TRIM(attributes_user_id) = '', 'invalid', attributes_user_id),
      mobile_number = IF(mobile_number IS NULL OR TRIM(mobile_number) = '', 'invalid', mobile_number),
      final_fare = IFNULL(final_fare, 0),
      price_discount = IFNULL(price_discount, 0)
    WHERE TRUE
    """
    query_job = client.query(query)
    query_job.result()
    print("BigQuery null cleanup completed.")

if __name__ == "__main__":
    run_bq_update()
