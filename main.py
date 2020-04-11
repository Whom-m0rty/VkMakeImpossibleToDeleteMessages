import requests
import sqlite3

# # Создание таблицы
# cursor.execute("""CREATE TABLE messages
#                   (id text, text text, attachment text,
#                    fwd text, message_id text)
#                """)

from cfg import token

last_message_id = 0

count = '1'
user_id = '348943165'
base_url = 'https://api.vk.com/method/messages.getHistory?'
conn = sqlite3.connect("mesages.db")  # или :memory: чтобы сохранить в RAM
cursor = conn.cursor()


def get_messages():
    try:
        response = requests.get(base_url + 'count=' + count + '&user_id=' + user_id +
                                '&access_token=' + token + '&v=5.103')
        response = response.json()
        from_id = response['response']['items'][0]['from_id']
        if str(from_id) == user_id:
            return response
        else:
            return None
    except:
        return None


def get_text(message):
    text = message['response']['items'][0]['text']
    return text


def get_attachments(message):
    attachment = message['response']['items'][0]['attachments']
    return attachment


def get_fwd(message):
    fwd = message['response']['items'][0]['fwd_messages']
    return fwd


def get_message_id(message):
    id = message['response']['items'][0]['conversation_message_id']
    return id


def parser(last_message_id):
    messages = get_messages()
    if messages:
        message_id = get_message_id(messages)
        if str(message_id) != str(last_message_id):
            text = get_text(messages)
            attachment = get_attachments(messages)
            fwd = get_fwd(messages)
            cursor.execute("""INSERT INTO messages
                                   VALUES (?, ?, ?,
                                   ?, ?)""", (user_id, str(text), str(attachment), str(fwd), message_id))
            conn.commit()


def start():
    while True:
        try:
            cursor.execute("SELECT * FROM messages")
            last_message_id = cursor.fetchall()[-1][-1]
            parser(last_message_id)
            print(last_message_id)
        except KeyboardInterrupt:
            conn.close()
            print('Курсор закрыт!')
            exit(1)


start()
