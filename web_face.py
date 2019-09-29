# -*- coding: utf-8 -*-
from flask import Flask
from flask import request
from flask import render_template


app = Flask(__name__)


@app.route("/")
def home():
    return render_template('test.html', test="test text")
    # return """<title>Hello, World!</title>
    #         <h1>Flask is ok</h1>
    #         <form method="POST" action="/send">
    #         <input type="submit" name="button" value="do smth">
    #         </form>
    #         """


@app.route("/send", methods=["POST"])
def send_1():
    print(request)
    return """<title>Hello, World!</title>
            <h1>Flask is ok</h1>
            <form method="POST" action="/">
            <input type="submit" name="button" value="do nothing">
            </form>
            """


@app.route("/", methods=["POST"])
def send_2():
    print(request)
    return """<title>Hello, World!</title>
            <h1>Flask is ok</h1>
            <form method="POST" action="/send">
            <input type="submit" name="button" value="do smth">
            </form>
            """


if __name__ == "__main__":
    app.run()