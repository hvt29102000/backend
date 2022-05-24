def make_api_response(status, data, message, **kwargs):
    resObj = {
        "status": status,
        "data": data,
        "message": message
    }
    for key, value in kwargs.items():
        resObj[key] = value
    return resObj
