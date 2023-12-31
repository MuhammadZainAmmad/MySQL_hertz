import base64
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account

def service_account_login():
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    client_credentials = service_account.Credentials.from_service_account_info({
        "type": "service_account",
        "project_id": "stayloom",
        "private_key_id": "5b8181b3f0b5ffccf65a10f8a43adccb4f3d202d",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKGRVqt6MqnLTI\nAO9fVzcgKPGljrDlTVwUP15IURAAen5HvcvTvYsLTg9K+5Dgv9q23ETPMie9m3TA\nKpINGq12QIDQrFTkXiWdlLTsTjrN87YnH+KB7k6xIN4e9/o5a22tBYseNzmJfi+N\nr7d5APi/bbcD/9VwYbuK2cVBH745/QTThB3orXPnoeBl4iXu1uxzuf4BERbQN7Kp\ng5gmMl3nJrJzcQL4Dm4zH42JCD1uyr6r+kPnSb4/V4P1vZsFeBHlNK/B8kgl6g4f\nvGuQODF43Ht8sAYnYwGskh/vBwYcTe7VxgGpQT125s6nhFPl/heeoa6Eksh1JnTN\nfiXZQXTVAgMBAAECggEABGfS4L8gLABh0rKHuIbkvhgUzN4rfiAqcVjAfY6VoCql\nBvujJ+c/BfZqaaD9k3l5maVh5mtkB7BBHAq0uttM3zuAo18YwNt9i4eCzY9W0FQ8\nMXLZCiopMuRlLmMk/dK/jHdG64tA8mQ0dXmYZA3XN+/hS4TGJ06pTjMfmryuQqjQ\neuyfYZWlOHJx9PY2hhn1WxsopjSZzHNkqh8cOECc0EI2EstMX0V97e/CH9Gr+AEh\n7fDvczudsgQMmA6DTrmwWpRBWSzG0BS8hVg+Rb0wIQHNvVMSFLJzF5tV1Rq1Utwi\n3ADAur17PmgQk2TUHvlZnTFzKDLkmLR+KPMOxyYLkQKBgQDr9+tNJ+EsfT0i8hpH\nq71+KuBU30ckuVKiqu7RWqS2vZc+yd8GDeZAkZHg/NOer/rZuRUY4SfQes/Wb7UR\nuTox5tfbGrAbvnYaVwPQRKR9EUmYrW/pIJ6RrQZz0kzax2nIvMWUWyxpbjtIQNvf\n7bTsr22uoGc0ow5StyoihizGEQKBgQDbQRb5KMJjAyHkeD0vw6BsIbZlJQqItRps\nLRp7GA/ODUC6Uzbyjc8MhUU8BTRTnX3PajTPiQBMdsfFuyl4Cz259Q+fJpYnMoCc\nmjyWB7vUxGgGOLFsXmB8teufIbpxJlhFAD6OK3NvQCMFaNEE3M/rvDiWOR2FUxYR\n3OmYlG+uhQKBgCt700uzlqYpKhP/g2JDvra6Vf6t6qFU2WqKj1nbF1FpnK6Aau3l\nr9GkQbqxPJoYmeR3W/DqxPiBOT2t9jMe7B94B70jrOJf9cmi0VwW2i0F+4b8JwxR\n64ay2OaNEYabit3oE4zkREnle100PpCEcHvRVCgC/SHRDnmlsUkNasDRAoGAf7U5\nCWXpW7yuWCKFGTYsUe+NCvr5WMmMG2hmHT7VreJgSmdAASYCbLuPqTcq1G1Oo6qs\nGholl2Q0VoL+05JQoOkR8VSLb0dmTFE2avkUOgkwwjbxeTq7nshj9uuxaki4b3CF\n/09lzG4iN/tmjBuF7DxVBYM9I7RSjZMMaThEmPUCgYAlRBbpbFfmU++OvRqhzJ9s\nCFKAQcfZ1z+ui4nYBWrIbxd0Tzp8fVzPALhsUFSQJ3EWzKvCoXYsCXsfiT8ZMbmz\nhioLdk4Cd5L4SGQBPVEflj93y02u+rkN2vp8wHa53W6i1PRSkhSfOR2FB9PQ+95J\nOB1wM9NbZHymbTRvKnW6ZA==\n-----END PRIVATE KEY-----\n",
        "client_email": "stayloom-etl-3@stayloom.iam.gserviceaccount.com",
        "client_id": "101951561882038605426",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/stayloom-etl-3%40stayloom.iam.gserviceaccount.com"
    }, )
    credentials = client_credentials.with_scopes(
        ['https://www.googleapis.com/auth/gmail.send']
    )
    delegated_credentials = credentials.with_subject('it@stayloom.com')
    service = build('gmail', 'v1', credentials=delegated_credentials)
    return service

html = """<!DOCTYPE html>
            <html>
            <body>
                <h1>My First Heading</h1>
                <p>My first paragraph.</p>
            </body>
            </html>
        """
service=service_account_login()
message = MIMEText(html,'html')
message['to'] = 'zain.at.hertz@gmail.com'
message['from'] = "it@stayloom.com"
message['subject'] = "Test"
message = (service.users().messages().send(userId="me", body={'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}).execute())
print(message)
