import psycopg2
from alive_progress import alive_bar


def get_billing_account(conn, organization_id):
    sql = """select id
             from accounts
             where organization_id = %s"""
    cursor = conn.cursor()
    cursor.execute(sql, (organization_id,))
    data = cursor.fetchone()
    cursor.close()
    return data


def insert_billing_account(conn, organization_id):
    sql = """INSERT INTO accounts (organization_id, name, document_code_type_id, currency)
             VALUES (%s, 'Default', 1, 'CLP') RETURNING id"""

    cursor = conn.cursor()
    cursor.execute(sql, (organization_id,))
    client_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return client_id


def get_organizations(conn):
    sql = ("select id, fullname, is_person from organizations where id not in "
           "('b585b9f4-994c-455e-91f8-15b942440c28', 'da87b3b5-2035-499d-bcc9-74f5419dae93', '5eacd889-4a81-4766-91f3-ea3067aa95d7')")
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    documental_conn = psycopg2.connect(database="documental_layer",
                                       host="10.2.0.2",
                                       user="documental_layer",
                                       password="vKA5h2CZmf1p6EgQKBPRn8YpZDTmh2ay",
                                       port="5432", application_name="manti-python-script")

    fincore_conn = psycopg2.connect(database="fincore",
                                    host="10.2.0.2",
                                    user="fincore",
                                    password="2Ne2QWkf2A9VKegk10ZetkcrUiKcui7mNmFYQwCE",
                                    port="5432", application_name="manti-python-script")

    organizations = get_organizations(documental_conn)

    with alive_bar(len(organizations), force_tty=True) as bar:
        for organization in organizations:
            organization_id = organization[0]
            if get_billing_account(fincore_conn, organization_id) is None:
                insert_billing_account(fincore_conn, organization_id)
            bar()

    documental_conn.close()
    fincore_conn.close()
