from flask import Flask
from flask_restful import Api
from resources import Twitter

app = Flask(__name__)

api = Api(app)

@app.route("/")
def home():
    return "<h1 style='color:blue'> This is the Medium Blogger Discovery script </h1>"


api.add_resource(Twitter, "/medium_blogger")

if __name__=="__main__":
    app.run(host="0.0.0.0", debug=True)