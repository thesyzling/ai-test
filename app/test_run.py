from main import execute, AppModel

class Request:
    def __init__(self, prompt):
        self.prompt = prompt

class Response:
    def __init__(self):
        self.message = ""

request = Request("Bir uzay gemisi çiz")
response = Response()
model = AppModel(request, response)

execute(model)
print(response.message)
