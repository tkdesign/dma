from flask import Response
import hashlib

USERS = {
    'employee': 'e10adc3949ba59abbe56e057f20f883e'  # md5('123456')
}

def check_auth(username, password):
    password_hash = hashlib.md5(password.encode()).hexdigest()
    return USERS.get(username) == password_hash


def authenticate():
    return Response(
        'Could not verify your access level for that URL.\n', 401,
        {'WWW-Authenticate': 'Basic realm="Restricted Area"'}
    )