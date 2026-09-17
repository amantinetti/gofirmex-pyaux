import psycopg2
from alive_progress import alive_bar


def get_pending_docs(conn):
    sql = """ select id, portfolio_id
              from sign_documents
              where notary_id is null
                and notary_signed_at is not null """
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    return documents


def get_pending_portfolios_ids(conn):
    sql = """ select portfolio_id
              from sign_documents
              where notary_id is null
                and notary_signed_at is not null
              group by portfolio_id """
    cursor = conn.cursor()
    cursor.execute(sql, )
    documents = cursor.fetchall()
    cursor.close()

    return documents


def get_portfolio(conn, portfolio_id):
    sql = """ select notary_id
              from portfolios
              where id = %s
                and notary_id is not null"""
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
