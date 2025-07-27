
import pandas as pd
import boto3
import pyarrow.parquet as pq
import redshift_connector

def load_data_to_redshift(df, table_name, conn_params):
    conn = redshift_connector.connect(**conn_params)
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(f'''
            INSERT INTO {table_name} (package_id, origin, destination, cost, delivery_time, sla_met)
            VALUES ('{row.package_id}', '{row.origin}', '{row.destination}', {row.cost}, {row.delivery_time}, {row.sla_met});
        ''')
    conn.commit()
    cursor.close()
    conn.close()

def run_etl():
    df = pd.read_csv("ecommerce_logistics_data.csv")
    df['cost'] = df['distance_km'] * 0.5 + df['weight_kg'] * 0.2
    df['sla_met'] = df['delivery_time'] <= df['sla_minutes']

    conn_params = {
        'host': 'your-redshift-cluster.us-west-2.redshift.amazonaws.com',
        'database': 'dev',
        'user': 'awsuser',
        'password': 'mypassword'
    }

    load_data_to_redshift(df, "logistics_metrics", conn_params)

if __name__ == "__main__":
    run_etl()
