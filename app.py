import json

from flask import Flask, request

from static.set_json_imp import set_json_impl

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/setJson', methods=['POST', 'GET'])
def set_json():
    data = request.form.get('data')
    print(json.loads(data))
    set_json_impl(data)
    return 'czp jian fei'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
