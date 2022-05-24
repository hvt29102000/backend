from flask import Flask, request, Response, jsonify, make_response
import os
import json
from pymongo import MongoClient
from model.ApiResponse import make_api_response
from flask_socketio import SocketIO, emit, send
import time
from pyspark.sql import SparkSession
from CFAlgo import CF
from import_mongo import readFromMongo
from kafka import KafkaProducer, TopicPartition
app = Flask(__name__)


DB_URI = "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
app.config['MONGO_URI'] = DB_URI
mongoClient = MongoClient(DB_URI)
db = mongoClient.movielens
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "movies"


COL_MOVIES = "movies_new"
COL_RATINGS = "ratings_copy"


@app.route("/", methods=["GET"])
def default():
    return make_api_response(200, "Big Data 2021", "OK")


@app.route("/movie", methods=["GET"])
def getMovieList():
    coll = db[COL_MOVIES]
    limit = int(request.args.get("limit")) if (
        request.args.get("limit")) else 100
    offset = int(request.args.get("offset")) if (
        request.args.get("offset")) else 0
    resultCursor = coll.find(
        {}, {'_id': False}).skip(offset).limit(limit)

    listResult = []
    for doc in resultCursor:
        listResult.append(doc)
    totalDocument = coll.count()
    result = make_api_response(
        "OK", listResult, "Lấy danh sách phim thành công", total=totalDocument)
    return jsonify(result)


@app.route("/unrated-movie", methods=["GET"])
def getUnratedUserByUserId():
    coll = db[COL_MOVIES]
    rating_col = db[COL_RATINGS]
    limit = int(request.args.get("limit")) if (
        request.args.get("limit")) else 100
    offset = int(request.args.get("offset")) if (
        request.args.get("offset")) else 0
    userId = request.args.get("userId")
    if userId:
        ratedUser = rating_col.find({"userId": userId}, {"_id": False})
        ratedMovieId = [str(x["movieId"]) for x in ratedUser]
        print(ratedMovieId)
        result=list(coll.find(
            {"movieId": {"$nin": ratedMovieId}}, {'_id': False}).skip(offset).limit(limit))
        totalDocument=coll.count()
        return make_api_response(
            200, result, "Lấy danh sách phim thành công", total = totalDocument)
    else:
        return make_api_response(
            401, result, "userId không hợp lệ")


# @ app.route("/convert", methods=["GET"])
# def convert():
#     coll = db[COL_RATINGS]
#     data = coll.find({})
#     for item in data:
#         item["rating"] = float(item["rating"])
#         coll.save(item)
#     return make_api_response(200, [], "OK")


@ app.route("/ratings-avg", methods = ["GET"])
def getUserRatingHistory():
    coll=db[COL_RATINGS]
    limit=int(request.args.get("limit"))
    data=list(coll.aggregate([{"$group": {"_id": "$movieId", "averageRating": {
                "$avg": "$rating"}}}, {"$sort": {"averageRating": -1}}, {"$limit": limit if limit else 100}]))
    return make_api_response(200, data, "OK", total = len(data))


@ app.route("/user/ratings", methods = ["GET"])
def getRatingsByUser():
    coll=db[COL_MOVIES]
    rating_col=db[COL_RATINGS]
    limit=int(request.args.get("limit")) if (
        request.args.get("limit")) else 100
    offset=int(request.args.get("offset")) if (
        request.args.get("offset")) else 0
    userId=request.args.get("userId")
    if userId:
        ratedUser=rating_col.find({"userId": userId}, {"_id": False})
        ratedMovieId=[str(x["movieId"]) for x in ratedUser]
        # print(ratedMovieId)
        result=list(coll.find(
            {"movieId": {"$in": ratedMovieId}}, {'_id': False}).skip(offset).limit(limit))
        totalDocument=coll.count()
        return make_api_response(
            200, result, "Lấy danh sách phim thành công", total = totalDocument)
    else:
        return make_api_response(
            401, result, "userId không hợp lệ")


@ app.route("/movie/ratings", methods = ["GET", "POST"])
def getMovieRatings():
    coll=db[COL_RATINGS]
    if (request.method == "GET"):
        movieId=float(request.args.get("movieId"))
        params={
            "movieId": movieId
        }
        result = list(coll.find({"movieId": movieId}, {"_id": False}))
        total = coll.count()
        result = make_api_response(
            "OK", result, "Lay danh sach danh gia thanh cong", total = total)
        return jsonify(result)
    else:
        body=request.json
        params=["movieId", "rating", "userId"]
        for key in body:
            if key not in params:
                return make_api_response(403, [], "body invalid")
        body["userId"]=str(body["userId"])
        body['timestamp']=round(time.time())
        coll.insert_one(body)

        return make_api_response(200, [], "OK")

# @app.route("/user/ratings")


@ app.route("/user/recommend", methods = ["GET"])
def getUserRecommendation():
    coll=db["recommendation"]
    collMovies=db[COL_MOVIES]
    userId=request.args.get("userId")
    print(userId)
    if userId:
        result=coll.find({"UserId": userId}, {"_id": False})
        recMovies=[x["Recommendation"] for x in result]
        print(recMovies)
        if len(recMovies) > 0:
            movies=[]
            for movieId in recMovies[0]:
                movie=list(collMovies.find(
                    {"movieId": movieId}, {"_id": False}))
                movies.append(movie[0])
            # movies = list(collMovies.find(
            #     {"movieId": {"$in": recMovies[0]}}, {"_id": False})))
            return make_api_response(200, movies, "OK", total = len(movies))
        else:
            return make_api_response(200, [], "OK", total = 0)
    else:
        return make_api_response(200, [], "OK", total = 0)


socketio=SocketIO(app, cors_allowed_origins = "*")


@ app.route("/ratings", methods = ["GET"])
def getAllRatings():
    coll=db[COL_RATINGS]
    limit=int(request.args.get("limit")) if "limit" in request.args else None
    result=list(coll.find({}, {"_id": False}).limit(limit))
    return make_api_response(200, result, "OK", total = len(result))


@ socketio.on('connect')
def test_connect():
    emit('my response', {'data': 'Connected'})


@ socketio.on("message", namespace = "/kafka")
def handleMessage(msg):
    print(msg)
    print(TOPIC_NAME)
    print(BOOTSTRAP_SERVERS)
    producer=KafkaProducer(bootstrap_servers = BOOTSTRAP_SERVERS,
                             value_serializer = lambda x: json.dumps(x).encode('utf-8'))
    producer.send(TOPIC_NAME, msg)


if __name__ == '__main__':
    app.host = 'localhost'
    app.debug = True
    socketio.run(app, port=5555)
