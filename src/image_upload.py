import os
import cb_utils
import psycopg2
from tqdm import tqdm
import argparse
from time import time

DEBUG = False


def upload_images(outputs_dir):
    large_files = os.listdir(f'outputs/{outputs_dir}/large')

    analytics_member_ids = [int(file_name.split('_')[0]) for file_name in large_files]
    conn = cb_utils.get_member_doc_conn()
    patient_analytics_member_ids = get_patient_analytics_mapping(conn, analytics_member_ids)

    for p_id, m_id in tqdm(patient_analytics_member_ids, desc='Uploading DS Images'):
        if DEBUG:
            print(f'Patient: {p_id}, Member: {m_id}')
        small = open(f'outputs/{outputs_dir}/small/{m_id}_small.png', 'rb').read()
        small_pmpms = open(f'outputs/{outputs_dir}/small_pmpms/{m_id}_small.png', 'rb').read()
        large = open(f'outputs/{outputs_dir}/large/{m_id}_ds.png', 'rb').read()
        upload_image(conn, p_id, small, large, small_pmpms )

    conn.commit()


def get_patient_analytics_mapping(conn, analytics_member_ids):
    query = """
        SELECT
            id patient_id, analytics_member_id
        FROM patients
        WHERE analytics_member_id = Any(%(analytic_member_ids)s);
        """

    mapping_cur = conn.cursor()
    mapping_cur.execute(query, {'analytic_member_ids': analytics_member_ids})

    results = mapping_cur.fetchall()
    mapping_cur.close()
    return results


def upload_image(conn, patient_id, small, large, small_pmpms ):
        cur = conn.cursor()
        query = """
        WITH
            deleted AS (
                DELETE FROM ds_images WHERE patient_id = %s
            )
        INSERT
        INTO
            ds_images(patient_id, large, small, small_pmpm, inserted_at, updated_at)
        VALUES
            (%s, %s, %s, %s, now(), now());
        """

        cur.execute(
            query,
            (patient_id, patient_id, psycopg2.Binary(large), psycopg2.Binary(small), psycopg2.Binary(small_pmpms))
        )
        cur.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload graphs for members")

    parser.add_argument('--output_path', help='Outputs Directory ie uhc_20201014 ("outputs/" is auto prepended)', required=False)

    args = parser.parse_args()

    s = time()
    upload_images(args.output_path)
    e = time()
    print('Finished uploading')
    cb_utils.printt(e-s)
