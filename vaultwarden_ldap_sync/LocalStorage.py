import time
import logging
import mysql.connector
from typing import Tuple, List

ALLOWED_USER_STATES = ['ENABLED', 'DISABLED', 'DELETED']


class LocalStore:

    def __init__(self, user: str, password: str, host: str, database: str):
        self.con = mysql.connector.connect(user=user, password=password, host=host, database=database)

    def get_all_users(self) -> Tuple[dict, dict, dict]:
        cursor = self.con.cursor()
        cursor.execute("SELECT invite_email, vw_email, vw_user_id, state FROM Users;")
        enabled = {}
        disabled = {}
        for invite_email, vw_email, vw_user_id, state in cursor.fetchall():
            if state == 'ENABLED':
                enabled[vw_user_id] = {'vw_email': vw_email, 'invite_email': invite_email}
            else:
                disabled[vw_user_id] = {'vw_email': vw_email, 'invite_email': invite_email}
        return enabled, disabled, {**enabled, **disabled}

    def register_user(self, user_email: str, user_id: str):
        cursor = self.con.cursor()
        try:
            cursor.execute(
                'INSERT INTO Users (invite_email, vw_email, vw_user_id, last_touched, state) VALUES (%s,%s,%s,%s,%s)',
                (user_email, user_email, user_id, time.strftime('%Y-%m-%d %H:%M:%S'), 'ENABLED'))
            self.con.commit()
        except mysql.connector.IntegrityError as e:
            logging.warning(f'Could not insert user {user_email}: {e}')

    def set_user_state(self, vw_user_id: str, user_state: str):
        if user_state not in ALLOWED_USER_STATES:
            raise ValueError(f'Invalid user state. Must be one of: {ALLOWED_USER_STATES}')
        else:
            cursor = self.con.cursor()
            cursor.execute('UPDATE Users SET state = %s, last_touched = %s WHERE vw_user_id = %s',
                           (user_state, time.strftime('%Y-%m-%d %H:%M:%S'), vw_user_id))
            self.con.commit()

    def update_vw_email(self, vw_user_id: str, new_vw_email: str):
        cursor = self.con.cursor()
        cursor.execute('UPDATE Users SET vw_email = %s, last_touched = %s WHERE vw_user_id = %s',
                       (new_vw_email, time.strftime('%Y-%m-%d %H:%M:%S'), vw_user_id))
        self.con.commit()

    def delete_user(self, vw_user_id: str):
        cursor = self.con.cursor()
        cursor.execute('DELETE FROM Users WHERE vw_user_id = %s', (vw_user_id,))
        self.con.commit()

    def __del__(self):
        self.con.close()
