import psycopg2
from alive_progress import alive_bar


def get_docs_pending_calc(conn):
    sql = """
          WITH np_norm AS (SELECT np.*,
                                  NULLIF(np.client_ids, '{}'::uuid[])         AS client_ids_n,
                                  NULLIF(np.signing_type_ids, '{}'::bigint[]) AS signing_type_ids_n
                           FROM notary_pricing np)
          SELECT sign_documents.id,
                 np_pick.id AS notary_pricing_id
          FROM sign_documents
                   join portfolios on portfolios.id = sign_documents.portfolio_id
                   JOIN LATERAL (
              SELECT np.*
              FROM np_norm np
              WHERE np.notary_id = sign_documents.notary_id
                -- “aplica” si no restringe o si contiene el valor
                AND (np.client_ids_n IS NULL OR portfolios.client_id = ANY (np.client_ids_n))
                AND (np.signing_type_ids_n IS NULL OR
                     sign_documents.notary_signing_type_id = ANY (np.signing_type_ids_n))
              ORDER BY
                  -- score: cuántos parámetros “especifica” (más específico gana)
                  ((np.client_ids_n IS NOT NULL)
              :: int +
             (np.signing_type_ids_n IS NOT NULL):: int) DESC
             ,

          -- desempates opcionales (si quieres preferir más “específico” aún)
              COALESCE (array_length(np.client_ids_n
             , 1)
             , 0) DESC
             , COALESCE (array_length(np.signing_type_ids_n
             , 1)
             , 0) DESC
             , np.id DESC
              LIMIT 1
              ) np_pick
          ON true
          where sign_documents.notary_pricing_id is null
            and sign_documents.notary_signed_at is not null
            and sign_documents.notary_id is not null"""

    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

def get_pending_docs(conn):
    sql = """select sign_documents.id,
                    sign_documents.portfolio_id,
                    sign_documents.notary_id,
                    portfolios.client_id,
                    sign_documents.notary_signing_type_id
             from sign_documents
                      join portfolios on portfolios.id = sign_documents.portfolio_id
             where notary_pricing_id is null
               and notary_signed_at is not null
               and sign_documents.notary_id is not null"""
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    return documents


def get_pricing(conn):
    sql = """select id, notary_id, client_ids, signing_type_ids
             from notary_pricing
          """
    cursor = conn.cursor()
    cursor.execute(sql, (portfolio_id,))
    portfolio = cursor.fetchone()
    cursor.close()

    return portfolio


def update_notary_id(conn, portfolio_id, notary_id):
    sql = "update sign_documents set notary_id = %s where portfolio_id = %s and notary_id is null and notary_signed_at is not null"
    cursor = conn.cursor()
    cursor.execute(sql, (notary_id, portfolio_id,))
    conn.commit()
    cursor.close()


if __name__ == '__main__':
    ## cloud-sql-proxy fex-v2:us-east1:main-layer-prod
    notary_conn = psycopg2.connect(database="notary_layer",
                                   host="10.2.0.2",
                                   user="notary_layer",
                                   password="YsB7cV9LWWDA4LenaGDsCCRz06fevi",
                                   port="5432", application_name="manti-python-script")

    documents = get_pending_portfolios_ids(notary_conn)

    with alive_bar(len(documents), force_tty=True) as bar:
        for document in documents:
            portfolio_id = document[0]
            notary_id = get_portfolio(notary_conn, portfolio_id)[0]
            update_notary_id(notary_conn, portfolio_id, notary_id)
            print("Updated Portfolio ID:{}, Notary ID:{}".format(portfolio_id, notary_id))
            bar()

    notary_conn.close()
